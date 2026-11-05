//! The scheduler: spawns every node the plan allows to run, fans each
//! node's streams out to its sinks as they are produced, propagates
//! failures, and collects the whole run into a [`Report`].
//!
//! The one thing this must never do is wait for a "stage" or "layer" to
//! finish. A node is spawned the instant its dependencies permit it, and
//! nothing else in the graph is held up while it runs.

use std::collections::HashMap;
use std::io::{self, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::exec::child::{spawn, stdio_for};
use crate::exec::signals;
use crate::exec::sinks::{SinkCtx, build_sink};
use crate::exec::tee::{SinkOutcome, Tee, TeeOutcome};
use crate::graph::{Fanout, NodeId, Plan, SinkSpec, StdinSource};
use crate::pb;
use crate::report::{Outcome, Report};

/// SIGPIPE's number — the death a producer takes when the orchestrator
/// drops its read end of that producer's stream.
const SIGPIPE: i32 = nix::sys::signal::Signal::SIGPIPE as i32;

/// Runs `plan` to quiescence and reports what every node did.
///
/// Pass-through output goes to the process's own stdout/stderr as it is
/// produced; the returned report is the caller's to print (to stderr) and
/// to turn into an exit code.
pub async fn run(plan: &Plan) -> Report {
    Runner::new(plan).run().await
}

/// How a node's process ended, as far as the orchestrator could tell.
#[derive(Debug, Clone, Copy)]
enum ExitObserved {
    Status(ExitStatus),
    /// The process ended but `wait` itself failed, so there is no status to
    /// report. Counts as a failure: it certainly did not succeed.
    Unknown,
}

#[derive(Debug, Default)]
struct NodeState {
    /// True once the process exists. A data-edge consumer becomes runnable
    /// on this, not on its producer's exit.
    started: bool,
    /// The process group this node's child was placed in, set the moment
    /// it is spawned. `shutdown` signals this, not the pid directly, so a
    /// child's own forked descendants are reached too.
    pgid: Option<i32>,
    exit: Option<ExitObserved>,
    /// Whether this node's exit lets its order-edge dependents run. Only
    /// meaningful once `exit` is set.
    sched_ok: bool,
    /// An outcome known without waiting for anything else: a skip, or a
    /// failure to spawn at all.
    decided: Option<Outcome>,
    /// The upstream node whose failure cut this node's stdin short.
    truncated_by: Option<String>,
    /// One of this node's streams had lost its whole *graph* audience when
    /// the node exited, so a nonzero exit means it was abandoned rather than
    /// broken. See [`Audience`] for what does and does not count.
    abandoned: bool,
    /// A destination that failed mid-stream, with the error — the node's
    /// output went on the floor even though the node itself exited 0.
    lost_output: Option<(String, String)>,
    /// Set when this node took SIGPIPE because the *orchestrator's* own
    /// stdout or stderr had gone away, naming it. Sampled at the instant of
    /// exit, like `abandoned`, and for the same reason.
    passthrough_gone: Option<String>,
}

/// What became of one fan-out's set of destinations, as far as deciding
/// whether its producer was *abandoned* goes.
///
/// "Its last consumer went away" is a statement about the graph. Three
/// things are therefore not abandonment, however they empty the live set:
///
/// - a **pass-through or file** sink leaving. That is the orchestrator's own
///   plumbing failing — a broken stdout because we were piped into `head`,
///   a file sink hitting ENOSPC — and must never excuse a node's failure.
///   Tracked as `graph_consumers_only`.
/// - a sink **we cancelled** because some node failed. It did not walk out;
///   we shut it. Tracked as `cancelled`, which covers the cross-node case
///   too: the cancellation can belong to a different node's failure and
///   arrive before this producer's own exit is processed, where timing alone
///   cannot tell the two apart.
/// - a sink that is simply still there when the producer exits, which is
///   the ordinary case and is what `no_sinks_left` being clear means.
///
/// `cancelled` is deliberately **fan-out-wide**, not per-sink: one shut sink
/// disqualifies the whole fan-out. So a producer fanning out to `[c, d]`
/// where `c` legitimately walks out and `d` is skipped by an unrelated
/// failure is reported `failed` rather than `abandoned`. Accepted: the
/// error only ever runs toward calling a nonzero exit a failure, never
/// toward excusing one, and the run is already failing on `d`'s skip.
struct Audience {
    /// The fan-out's eager "no sinks left" flag (see `Tee::no_sinks_left`).
    no_sinks_left: watch::Receiver<bool>,
    /// Every destination of this fan-out is another node's stdin, so
    /// emptying the live set really is the graph walking out.
    graph_consumers_only: bool,
    /// The stream this fan-out carries, for the diagnostic below.
    stream: pb::Stream,
    /// Set when this fan-out's single destination is the orchestrator's own
    /// stdout or stderr, naming it.
    ///
    /// Only ever `Some` for a *pure* pass-through fan-out: the plan adds one
    /// only for a stream no data edge claimed, with exactly that one sink.
    /// So this and `graph_consumers_only` are never both set, and the two
    /// classifications they drive cannot collide.
    passthrough: Option<String>,
    /// Set when we cancel any of this fan-out's sinks.
    ///
    /// Shared with [`Runner::stdin_cancelled`] rather than reached through
    /// an index into the owning node's `audiences`. An index has to be kept
    /// in step with a `push` that a later sink's failure to open can skip,
    /// which is a panic waiting to happen; a handle created before any sink
    /// can register against it cannot get out of step with anything.
    cancelled: Arc<AtomicBool>,
}

impl Audience {
    /// Whether this fan-out losing its sinks is grounds for calling its
    /// producer abandoned.
    fn walked_out(&self) -> bool {
        self.graph_consumers_only
            && !self.cancelled.load(Ordering::Relaxed)
            && *self.no_sinks_left.borrow()
    }

    /// Whether this fan-out lost the orchestrator's own stdout/stderr, which
    /// is what makes dropping the source — and so the producer's SIGPIPE —
    /// our doing rather than the node's fault. Returns the stream that was
    /// closed and the departed destination's name.
    ///
    /// Sampled at the same instant as [`Self::walked_out`], and with the
    /// same `cancelled` guard, which here is belt and braces: a
    /// pass-through sink is never cancelled.
    fn passthrough_departed(&self) -> Option<(pb::Stream, &str)> {
        let destination = self.passthrough.as_deref()?;
        (!self.cancelled.load(Ordering::Relaxed) && *self.no_sinks_left.borrow())
            .then_some((self.stream, destination))
    }
}

/// Why one node waits for another.
#[derive(Debug, Clone, Copy)]
enum DepKind {
    /// The dependent reads the upstream node's output.
    Data,
    Order {
        any_exit: bool,
    },
}

#[derive(Debug, Clone, Copy)]
struct Dep {
    node: NodeId,
    kind: DepKind,
}

/// A running fan-out, so its outcome can be attributed to the node and
/// stream it belongs to.
struct TeeHandle {
    node: NodeId,
    stream: pb::Stream,
    handle: JoinHandle<TeeOutcome>,
}

/// A child's output pipe, whichever stream it came from.
enum SourceStream {
    Out(ChildStdout),
    Err(ChildStderr),
}

struct Runner<'p> {
    plan: &'p Plan,
    nodes: Vec<NodeState>,
    /// For each node, who waits on it and why.
    dependents: Vec<Vec<Dep>>,
    ctx: SinkCtx,
    /// The channel each node uses to hand its stdin pipe to the sink that
    /// feeds it, once it spawns.
    stdin_senders: HashMap<NodeId, oneshot::Sender<ChildStdin>>,
    /// Cancellation tokens for the sinks feeding not-yet-decided nodes'
    /// stdin. Cancelling one releases a producer that the node's unread
    /// queue was throttling.
    stdin_tokens: HashMap<NodeId, CancellationToken>,
    /// Per node, one [`Audience`] per stream it produces, so a node's exit
    /// can be classified the instant it happens.
    audiences: Vec<Vec<Audience>>,
    /// The "a sink of mine was cancelled" flag of whichever fan-out feeds a
    /// given node's stdin, so releasing that node's sink can be recorded
    /// against the fan-out that was feeding it. Shares the flag with that
    /// [`Audience`]; see its `cancelled` field for why this is a handle
    /// rather than an index.
    stdin_cancelled: HashMap<NodeId, Arc<AtomicBool>>,
    tees: Vec<TeeHandle>,
    notes: Vec<(String, String)>,
    exit_tx: mpsc::UnboundedSender<(NodeId, io::Result<ExitStatus>)>,
    exit_rx: mpsc::UnboundedReceiver<(NodeId, io::Result<ExitStatus>)>,
    running: usize,
    /// Set once SIGINT or SIGTERM is handled. Stops any further node from
    /// being spawned and forces the run to report nonzero regardless of
    /// how the nodes themselves turned out.
    interrupted: bool,
}

impl<'p> Runner<'p> {
    fn new(plan: &'p Plan) -> Self {
        let mut dependents: Vec<Vec<Dep>> = vec![Vec::new(); plan.nodes.len()];
        let mut stdin_senders = HashMap::new();
        let mut child_stdins = HashMap::new();

        for (i, spec) in plan.nodes.iter().enumerate() {
            let id = NodeId(i);
            for dep in &spec.order_deps {
                dependents[dep.after.0].push(Dep {
                    node: id,
                    kind: DepKind::Order {
                        any_exit: dep.any_exit,
                    },
                });
            }
            if let StdinSource::Node { from, .. } = &spec.stdin {
                dependents[from.0].push(Dep {
                    node: id,
                    kind: DepKind::Data,
                });
                // Registered up front, so the producer's fan-out can attach
                // this node's sink the moment the producer spawns, long
                // before the node itself is allowed to start.
                let (tx, rx) = oneshot::channel();
                stdin_senders.insert(id, tx);
                child_stdins.insert(id, rx);
            }
        }

        let (exit_tx, exit_rx) = mpsc::unbounded_channel();
        Self {
            plan,
            nodes: (0..plan.nodes.len())
                .map(|_| NodeState::default())
                .collect(),
            dependents,
            ctx: SinkCtx {
                child_stdins,
                stdout: Arc::new(Mutex::new(Box::new(io::stdout()))),
                stderr: Arc::new(Mutex::new(Box::new(io::stderr()))),
                tags: plan.tags,
            },
            stdin_senders,
            stdin_tokens: HashMap::new(),
            audiences: (0..plan.nodes.len()).map(|_| Vec::new()).collect(),
            stdin_cancelled: HashMap::new(),
            tees: Vec::new(),
            notes: Vec::new(),
            exit_tx,
            exit_rx,
            running: 0,
            interrupted: false,
        }
    }

    async fn run(mut self) -> Report {
        // One future for the whole run, polled from the select loop below
        // until it resolves once. A repeat SIGINT or SIGTERM during
        // shutdown is deliberately not handled a second time (the `if
        // !self.interrupted` guard stops polling it at all) — the grace
        // period and the eventual SIGKILL sweep are the only response a
        // second signal gets.
        let shutdown_signal = signals::wait_for_shutdown_signal();
        tokio::pin!(shutdown_signal);

        loop {
            // Spawning a node can make others runnable (a data consumer
            // waits only for its producer to *start*), so keep going until
            // nothing more can be spawned — then, and only then, wait. Once
            // interrupted, no further node is ever spawned.
            if !self.interrupted {
                while self.spawn_runnable_nodes() {}
            }

            if self.running == 0 {
                break;
            }

            tokio::select! {
                exit = self.exit_rx.recv() => {
                    let (id, status) = exit.expect("the runner holds a sender for the whole run");
                    self.running -= 1;
                    self.handle_exit(id, status);
                }
                () = &mut shutdown_signal, if !self.interrupted => {
                    self.handle_signal().await;
                }
            }
        }
        self.finish().await
    }

    /// Reacts to the first SIGINT/SIGTERM: stops scheduling (checked by the
    /// caller), forwards the signal to every live child's process group,
    /// and waits out the grace period before anything left gets SIGKILLed.
    /// `self.running` still only reaches zero once every per-child `wait`
    /// task reports its exit, so this never reaps anything itself — it
    /// only makes sure those exits actually happen soon.
    async fn handle_signal(&mut self) {
        self.interrupted = true;
        let live: Vec<(String, i32)> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(i, state)| {
                if state.started && state.exit.is_none() {
                    state
                        .pgid
                        .map(|pgid| (self.plan.node(NodeId(i)).name.clone(), pgid))
                } else {
                    None
                }
            })
            .collect();
        signals::shutdown(&live, self.plan.shutdown_grace).await;
    }

    /// Spawns every node that is runnable right now. Returns whether it
    /// spawned (or tried to spawn) anything.
    fn spawn_runnable_nodes(&mut self) -> bool {
        let runnable: Vec<NodeId> = (0..self.nodes.len())
            .map(NodeId)
            .filter(|&id| self.is_runnable(id))
            .collect();
        for id in &runnable {
            self.spawn_node(*id);
        }
        !runnable.is_empty()
    }

    /// A node is runnable when every order-edge predecessor has exited
    /// acceptably and, if its stdin comes from a node, that node has
    /// *started*. Deliberately not "finished": that would serialize the
    /// graph into stages.
    fn is_runnable(&self, id: NodeId) -> bool {
        let state = &self.nodes[id.0];
        if state.started || state.decided.is_some() {
            return false;
        }
        for dep in &self.plan.node(id).order_deps {
            let upstream = &self.nodes[dep.after.0];
            match upstream.exit {
                None => return false,
                Some(_) if !dep.any_exit && !upstream.sched_ok => return false,
                Some(_) => {}
            }
        }
        if let StdinSource::Node { from, .. } = &self.plan.node(id).stdin
            && !self.nodes[from.0].started
        {
            return false;
        }
        true
    }

    fn spawn_node(&mut self, id: NodeId) {
        let spec = self.plan.node(id);
        let stdin = match stdio_for(&spec.stdin) {
            Ok(stdin) => stdin,
            Err(e) => return self.spawn_failed(id, e),
        };

        // Wire up where this node's output is going *before* starting it: a
        // node whose declared destination cannot be opened must not run at
        // all, rather than run and have its results go nowhere.
        //
        // Known wart: if a *later* fan-out of the same node fails to open,
        // the file sinks already opened have been created and truncated by
        // then, so an empty output file can be left behind for a node that
        // never ran. The node is reported as having failed to start and the
        // run exits nonzero, so the file is never the only evidence — but
        // do not read such a file as "the node ran and produced nothing".
        let mut tees = Vec::new();
        for fanout in self.plan.fanouts.iter().filter(|f| f.source == id) {
            match self.build_tee(fanout) {
                Ok(tee) => tees.push((fanout.stream, tee)),
                Err(e) => return self.spawn_failed(id, e),
            }
        }

        let merged = self.has_stream(id, pb::Stream::Both);
        let want_stdout = merged || self.has_stream(id, pb::Stream::Stdout);
        let want_stderr = !merged && self.has_stream(id, pb::Stream::Stderr);
        let mut spawned = match spawn(spec, stdin, want_stdout, want_stderr, merged) {
            Ok(spawned) => spawned,
            Err(e) => return self.spawn_failed(id, e),
        };
        self.nodes[id.0].started = true;
        self.nodes[id.0].pgid = Some(spawned.pgid);

        if let Some(tx) = self.stdin_senders.remove(&id) {
            // Hands the write end of this node's stdin to the sink already
            // waiting to feed it. If that sink is gone (its tee finished
            // first), dropping the handle closes the pipe and the node
            // simply sees EOF.
            if let Some(stdin) = spawned.child.stdin.take() {
                let _ = tx.send(stdin);
            }
        }

        let stdout = spawned.child.stdout.take();
        let stderr = spawned.child.stderr.take();
        self.start_tees(id, tees, stdout, stderr);

        let mut child = spawned.child;
        let exits = self.exit_tx.clone();
        tokio::spawn(async move {
            let status = child.wait().await;
            let _ = exits.send((id, status));
        });
        self.running += 1;
    }

    /// A node that never came into existence. Its dependents cannot run —
    /// not even an `any_exit` order dependent, which is waiting for an exit
    /// that will never happen.
    fn spawn_failed(&mut self, id: NodeId, error: io::Error) {
        self.nodes[id.0].decided = Some(Outcome::SpawnFailed {
            message: error.to_string(),
        });
        // This node will never read its own stdin, so whatever is queued for
        // it must be released on exactly the grounds a skip uses. Without
        // this, a producer throttled by the queue of a consumer that failed
        // to start is wedged forever: its sink waits for a stdin pipe that
        // can never arrive, holding permits the tee needs, so the producer
        // never exits and the run never ends.
        self.release_stdin_sink(id);
        self.propagate_failure(id, id, false);
    }

    fn has_stream(&self, id: NodeId, stream: pb::Stream) -> bool {
        self.plan
            .fanouts
            .iter()
            .any(|f| f.source == id && f.stream == stream)
    }

    /// Hands each prepared fan-out the pipe it reads from and sets it
    /// running, so the node's bytes start moving the instant it produces
    /// them.
    fn start_tees(
        &mut self,
        id: NodeId,
        tees: Vec<(pb::Stream, Tee)>,
        mut stdout: Option<ChildStdout>,
        mut stderr: Option<ChildStderr>,
    ) {
        for (stream, tee) in tees {
            let src = match stream {
                pb::Stream::Stderr => stderr.take().map(SourceStream::Err),
                _ => stdout.take().map(SourceStream::Out),
            };
            let Some(src) = src else {
                let name = self.plan.node(id).name.clone();
                self.note(
                    &name,
                    format!("{} was never piped back", stream_label(stream)),
                );
                continue;
            };
            let handle = match src {
                SourceStream::Out(src) => tokio::spawn(tee.run(src)),
                SourceStream::Err(src) => tokio::spawn(tee.run(src)),
            };
            self.tees.push(TeeHandle {
                node: id,
                stream,
                handle,
            });
        }
    }

    /// Builds every sink of one fan-out and starts their consumer tasks.
    /// Fails if any destination cannot be opened: output that cannot be
    /// delivered is not something to discover halfway through a run.
    fn build_tee(&mut self, fanout: &Fanout) -> io::Result<Tee> {
        let plan = self.plan;
        let source = plan.node(fanout.source).name.clone();
        let mut tee = Tee::new(plan.queue_cap_bytes);
        let mut graph_consumers_only = true;
        let mut passthrough = None;
        // Created before the loop, so every sink that registers against it
        // below shares a flag that already exists. Nothing here depends on
        // this fan-out's `Audience` having been pushed — which the `?` below
        // can skip entirely — so a later sink failing to open cannot leave a
        // dangling reference behind.
        let cancelled = Arc::new(AtomicBool::new(false));

        for spec in &fanout.sinks {
            let label = sink_label(plan, spec);
            let sink = build_sink(spec, &mut self.ctx, &source).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("{} cannot reach {label}: {e}", stream_label(fanout.stream)),
                )
            })?;
            let token = tee.add_sink(label, sink);

            // Only a node-stdin sink may ever be cancelled. Cancelling a
            // file sink mid-write would leave a torn tail on disk, and a
            // passthrough sink's bytes are output somebody wants; the
            // "nobody will read it anyway" argument holds for a skipped
            // node's stdin and nowhere else.
            match spec {
                SinkSpec::NodeStdin(consumer) => {
                    self.stdin_cancelled.insert(*consumer, cancelled.clone());
                    if self.nodes[consumer.0].decided.is_some() {
                        // Already skipped or failed to start before its
                        // producer even began; release the queue now rather
                        // than let it fill for nobody.
                        token.cancel();
                        cancelled.store(true, Ordering::Relaxed);
                    } else {
                        self.stdin_tokens.insert(*consumer, token);
                    }
                }
                SinkSpec::File { .. } => graph_consumers_only = false,
                SinkSpec::Passthrough(_) => {
                    graph_consumers_only = false;
                    passthrough = Some(sink_label(plan, spec));
                }
            }
        }

        self.audiences[fanout.source.0].push(Audience {
            no_sinks_left: tee.no_sinks_left(),
            graph_consumers_only,
            stream: fanout.stream,
            passthrough,
            cancelled,
        });
        Ok(tee)
    }

    /// Releases the sink feeding `consumer`'s stdin, because `consumer` will
    /// never read it. Recorded against the fan-out that was feeding it: a
    /// sink we shut did not walk out, so it can never make its producer look
    /// abandoned.
    fn release_stdin_sink(&mut self, consumer: NodeId) {
        if let Some(token) = self.stdin_tokens.remove(&consumer) {
            token.cancel();
        }
        // Belt and braces. Cancelling the token above is already enough to
        // stop a `Pending` sink waiting for this node's stdin: `drain_sink`
        // races the token against the write, the oneshot await lives inside
        // that write, and `CloseOnDrop` frees the permits either way.
        // Dropping the sender simply closes the pipe for good.
        self.stdin_senders.remove(&consumer);
        if let Some(cancelled) = self.stdin_cancelled.get(&consumer) {
            cancelled.store(true, Ordering::Relaxed);
        }
    }

    fn handle_exit(&mut self, id: NodeId, status: io::Result<ExitStatus>) {
        let name = self.plan.node(id).name.clone();
        let observed = match status {
            Ok(status) => ExitObserved::Status(status),
            Err(e) => {
                self.note(&name, format!("could not observe how it ended: {e}"));
                ExitObserved::Unknown
            }
        };

        // Read *now*, before anything downstream of this exit happens: a
        // fan-out whose graph consumers had already walked out is what
        // abandons a producer, whereas sinks cancelled in response to this
        // very exit (just below) are a consequence of it and must not excuse
        // it. This is the same reading the summary will use, so what the
        // scheduler does and what the summary says can never disagree.
        let abandoned = self.audiences[id.0].iter().any(Audience::walked_out);

        // The other way we can be the proximate cause of a node's death:
        // our own stdout (or stderr) went away — we were piped into `head`
        // — which left a pass-through fan-out with no sinks, so the tee
        // dropped its read end and the node took SIGPIPE writing into it.
        //
        // Restricted to a SIGPIPE death on purpose. A node that chose its
        // own nonzero exit status failed on its own account, whatever
        // happened to our stdout, which is what keeps
        // `a_broken_orchestrator_stdout_does_not_excuse_a_failing_node`
        // true. Sampled here, with the same `cancelled` guard as
        // abandonment, so a sink we shut on another node's behalf can never
        // excuse anything.
        let passthrough_gone = match observed {
            ExitObserved::Status(status) if status.signal() == Some(SIGPIPE) => self.audiences
                [id.0]
                .iter()
                .find_map(Audience::passthrough_departed)
                .map(|(stream, destination)| (stream, destination.to_string())),
            _ => None,
        };
        if let Some((stream, destination)) = &passthrough_gone {
            self.note(
                &name,
                format!(
                    "{} was closed from our end once {destination} went away",
                    stream_label(*stream)
                ),
            );
        }

        let sched_ok = match observed {
            // Both of these count as success for scheduling: a producer
            // whose consumers all walked out, or whose only destination was
            // our own departed stdout, has done nothing wrong.
            ExitObserved::Status(status) => {
                status.success() || abandoned || passthrough_gone.is_some()
            }
            ExitObserved::Unknown => false,
        };
        self.nodes[id.0].exit = Some(observed);
        self.nodes[id.0].sched_ok = sched_ok;
        self.nodes[id.0].abandoned = abandoned;
        self.nodes[id.0].passthrough_gone = passthrough_gone.map(|(_, destination)| destination);

        if !sched_ok {
            self.propagate_failure(id, id, true);
        }
    }

    /// Walks everything downstream of a node that failed.
    ///
    /// A dependent that has not started is skipped, transitively, and the
    /// sink feeding its stdin is cancelled so the producer that its unread
    /// queue was throttling is released — otherwise a skipped node with a
    /// filling queue deadlocks the run. A dependent that is already
    /// running is left alone: it keeps its bytes, sees EOF where the stream
    /// was cut, and is recorded as having had truncated input.
    ///
    /// `root` is the node that actually failed, named as the cause of every
    /// skip in its wake. `upstream_exited` is false when `from` never ran at
    /// all, in which case even an `any_exit` order edge cannot be satisfied.
    fn propagate_failure(&mut self, from: NodeId, root: NodeId, upstream_exited: bool) {
        let because = self.plan.node(root).name.clone();
        for dep in self.dependents[from.0].clone() {
            let blocked = match dep.kind {
                DepKind::Data => true,
                DepKind::Order { any_exit } => !(any_exit && upstream_exited),
            };
            if !blocked {
                continue;
            }

            let state = &mut self.nodes[dep.node.0];
            if state.started {
                // Known limitation, per the spec, which describes the direct
                // case only: if this node goes on to exit 0 with truncated
                // input, its *own* consumers are not also marked truncated.
                // The run already fails, so the loss is in the detail of the
                // summary, not in the verdict.
                if matches!(dep.kind, DepKind::Data) && state.truncated_by.is_none() {
                    state.truncated_by = Some(because.clone());
                }
                continue;
            }
            if state.decided.is_some() {
                continue;
            }
            state.decided = Some(Outcome::Skipped {
                because: because.clone(),
            });

            self.release_stdin_sink(dep.node);
            self.propagate_failure(dep.node, root, false);
        }
    }

    /// Joins every fan-out, turns what they report into notes and
    /// abandonment, and assembles the report.
    async fn finish(mut self) -> Report {
        let plan = self.plan;

        // In a validated (acyclic) plan every node either starts or is
        // skipped by propagation, so absent a signal this changes nothing.
        // It is here because the alternative to deciding a stranded node is
        // hanging below, waiting for a fan-out whose stdin pipe will never
        // arrive. Once interrupted, this is in fact the normal way a
        // never-scheduled node gets decided: nothing failed it, scheduling
        // just stopped, so it gets its own outcome rather than a `Skipped`
        // whose `because` is meant to name a culprit node.
        for i in 0..plan.nodes.len() {
            let state = &self.nodes[i];
            if state.started || state.decided.is_some() || state.exit.is_some() {
                continue;
            }
            self.nodes[i].decided = Some(if self.interrupted {
                Outcome::Interrupted
            } else {
                Outcome::Skipped {
                    because: "an upstream node".to_string(),
                }
            });
            self.release_stdin_sink(NodeId(i));
        }

        // Every node has by now either started — handing its stdin pipe over
        // as it did — or been decided, so any sender still held here belongs
        // to a node that will never spawn. Dropping them closes those pipes,
        // which also means no fan-out below can wait on a handle that is
        // never coming.
        self.stdin_senders.clear();

        for TeeHandle {
            node,
            stream,
            handle,
        } in std::mem::take(&mut self.tees)
        {
            let name = plan.node(node).name.clone();
            let stream = stream_label(stream);
            // `early_stop` is deliberately not consulted here. It can also
            // become true *after* the node exited — a consumer leaving once
            // the producer is already gone — and reading it then would
            // excuse a node that had genuinely failed on its own. The flag
            // sampled at the moment of exit is the honest answer.
            let TeeOutcome {
                read_error, sinks, ..
            } = match handle.await {
                Ok(outcome) => outcome,
                Err(e) => {
                    self.note(&name, format!("the {stream} fan-out task failed: {e}"));
                    continue;
                }
            };

            // Known gap: neither of these two conditions is covered by an
            // end-to-end test. A mid-stream read failure on a child's own
            // pipe cannot be induced from a graph fixture without faking
            // I/O, and a sink write failure needs a destination that accepts
            // an open and then refuses the write (`/dev/full`, which exists
            // only on Linux and is used by one `cfg`-gated test). The
            // formatting and placement of both are unit-tested in
            // `report.rs`; the triggers are not.
            if let Some(e) = read_error {
                self.note(&name, format!("reading {stream} was cut short: {e}"));
            }
            for (sink, outcome) in sinks {
                if let SinkOutcome::Errored(e) = outcome {
                    self.note(&name, format!("writing {stream} to {sink} failed: {e}"));
                    // A node that exited 0 into a destination that then
                    // failed has had its results thrown away, which makes
                    // the *run* wrong on the same grounds as truncated
                    // input. First failure wins: one named destination is
                    // enough to act on.
                    let state = &mut self.nodes[node.0];
                    if state.lost_output.is_none() {
                        state.lost_output = Some((sink, e.to_string()));
                    }
                }
            }
        }

        // Everything written through a passthrough sink has landed by now;
        // make sure it is out of our buffers before the summary follows it.
        let _ = self.ctx.stdout.lock().unwrap().flush();
        let _ = self.ctx.stderr.lock().unwrap().flush();

        let mut entries = Vec::with_capacity(plan.nodes.len());
        for (i, spec) in plan.nodes.iter().enumerate() {
            entries.push((spec.name.clone(), self.outcome_of(NodeId(i))));
        }
        Report {
            entries,
            notes: self.notes,
            interrupted: self.interrupted,
        }
    }

    fn outcome_of(&self, id: NodeId) -> Outcome {
        let state = &self.nodes[id.0];
        if let Some(decided) = &state.decided {
            return decided.clone();
        }
        let status = match state.exit {
            Some(ExitObserved::Status(status)) => status,
            Some(ExitObserved::Unknown) => {
                return Outcome::Failed {
                    code: None,
                    signal: None,
                };
            }
            // Unreachable: `finish` decides every stranded node first.
            None => {
                return Outcome::Skipped {
                    because: "an upstream node".to_string(),
                };
            }
        };

        if status.success() {
            // Both of these say "the node is blameless but the run's results
            // are wrong". Truncated input comes first: an upstream failure
            // is the root cause and names the node to go and look at, where
            // a lost destination may well be a consequence of it.
            match (&state.truncated_by, &state.lost_output) {
                (Some(upstream), _) => Outcome::TruncatedInput {
                    upstream: upstream.clone(),
                },
                (None, Some((destination, error))) => Outcome::LostOutput {
                    destination: destination.clone(),
                    error: error.clone(),
                },
                (None, None) => Outcome::Succeeded,
            }
        } else if state.abandoned {
            Outcome::Abandoned
        } else if let Some(destination) = &state.passthrough_gone {
            Outcome::PassthroughGone {
                destination: destination.clone(),
            }
        } else {
            Outcome::Failed {
                code: status.code(),
                signal: status.signal(),
            }
        }
    }

    fn note(&mut self, node: &str, message: String) {
        self.notes.push((node.to_string(), message));
    }
}

fn sink_label(plan: &Plan, spec: &SinkSpec) -> String {
    match spec {
        SinkSpec::NodeStdin(id) => format!("{}'s stdin", plan.node(*id).name),
        SinkSpec::File { path, .. } => path.display().to_string(),
        SinkSpec::Passthrough(pb::Stream::Stderr) => "the orchestrator's stderr".to_string(),
        SinkSpec::Passthrough(_) => "the orchestrator's stdout".to_string(),
    }
}

fn stream_label(stream: pb::Stream) -> &'static str {
    match stream {
        pb::Stream::Stderr => "stderr",
        pb::Stream::Both => "stdout+stderr",
        pb::Stream::Stdout | pb::Stream::Unspecified => "stdout",
    }
}
