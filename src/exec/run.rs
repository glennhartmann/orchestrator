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
use std::sync::{Arc, Mutex};

use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::exec::child::{spawn, stdio_for};
use crate::exec::sinks::{SinkCtx, build_sink};
use crate::exec::tee::{SinkOutcome, Tee, TeeOutcome};
use crate::graph::{Fanout, NodeId, Plan, SinkSpec, StdinSource};
use crate::pb;
use crate::report::{Outcome, Report};

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
    exit: Option<ExitObserved>,
    /// Whether this node's exit lets its order-edge dependents run. Only
    /// meaningful once `exit` is set.
    sched_ok: bool,
    /// An outcome known without waiting for anything else: a skip, or a
    /// failure to spawn at all.
    decided: Option<Outcome>,
    /// The upstream node whose failure cut this node's stdin short.
    truncated_by: Option<String>,
    /// One of this node's streams lost its last sink of its own accord, so
    /// a nonzero exit means it was abandoned rather than broken.
    abandoned: bool,
    /// A sink of this node's own output was cancelled *because this node
    /// failed*. Losing its consumers that way must not be mistaken for the
    /// consumers walking out on a healthy producer.
    self_cancelled: bool,
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
    tees: Vec<TeeHandle>,
    notes: Vec<(String, String)>,
    exit_tx: mpsc::UnboundedSender<(NodeId, io::Result<ExitStatus>)>,
    exit_rx: mpsc::UnboundedReceiver<(NodeId, io::Result<ExitStatus>)>,
    running: usize,
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
            tees: Vec::new(),
            notes: Vec::new(),
            exit_tx,
            exit_rx,
            running: 0,
        }
    }

    async fn run(mut self) -> Report {
        loop {
            // Spawning a node can make others runnable (a data consumer
            // waits only for its producer to *start*), so keep going until
            // nothing more can be spawned — then, and only then, wait.
            while self.spawn_runnable_nodes() {}

            if self.running == 0 {
                break;
            }
            let (id, status) = self
                .exit_rx
                .recv()
                .await
                .expect("the runner holds a sender for the whole run");
            self.running -= 1;
            self.handle_exit(id, status);
        }
        self.finish().await
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
            if let SinkSpec::NodeStdin(consumer) = spec {
                if self.nodes[consumer.0].decided.is_some() {
                    // Already skipped before its producer even started;
                    // release the queue immediately rather than let it fill.
                    token.cancel();
                } else {
                    self.stdin_tokens.insert(*consumer, token);
                }
            }
        }
        Ok(tee)
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

        // Whether a nonzero exit was really an abandonment is only settled
        // once the node's fan-outs are joined, which can be much later. For
        // scheduling, death by SIGPIPE stands in: that is how a producer
        // whose last consumer went away dies.
        let sched_ok = match observed {
            ExitObserved::Status(status) => {
                status.success() || status.signal() == Some(nix::libc::SIGPIPE)
            }
            ExitObserved::Unknown => false,
        };
        self.nodes[id.0].exit = Some(observed);
        self.nodes[id.0].sched_ok = sched_ok;

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

            let producer = match &self.plan.node(dep.node).stdin {
                StdinSource::Node { from, .. } => Some(*from),
                _ => None,
            };
            if let Some(token) = self.stdin_tokens.remove(&dep.node) {
                token.cancel();
                // When the queue we just released was fed by the node that
                // failed, that is the failure costing itself its consumer —
                // not a consumer walking out, which is what abandonment is.
                if producer == Some(root) {
                    self.nodes[root.0].self_cancelled = true;
                }
            }
            self.propagate_failure(dep.node, root, false);
        }
    }

    /// Joins every fan-out, turns what they report into notes and
    /// abandonment, and assembles the report.
    async fn finish(mut self) -> Report {
        let plan = self.plan;

        // In a validated (acyclic) plan every node either starts or is
        // skipped by propagation, so this changes nothing. It is here
        // because the alternative to deciding a stranded node is hanging
        // below, waiting for a fan-out whose stdin pipe will never arrive.
        for i in 0..plan.nodes.len() {
            let state = &self.nodes[i];
            if state.started || state.decided.is_some() || state.exit.is_some() {
                continue;
            }
            self.nodes[i].decided = Some(Outcome::Skipped {
                because: "an upstream node".to_string(),
            });
            if let Some(token) = self.stdin_tokens.remove(&NodeId(i)) {
                token.cancel();
            }
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
            let TeeOutcome {
                early_stop,
                read_error,
                sinks,
            } = match handle.await {
                Ok(outcome) => outcome,
                Err(e) => {
                    self.note(&name, format!("the {stream} fan-out task failed: {e}"));
                    continue;
                }
            };

            if let Some(e) = read_error {
                self.note(&name, format!("reading {stream} was cut short: {e}"));
            }
            for (sink, outcome) in sinks {
                if let SinkOutcome::Errored(e) = outcome {
                    self.note(&name, format!("writing {stream} to {sink} failed: {e}"));
                }
            }
            // A stream that ran out of sinks is what abandons a producer —
            // unless it ran out because this very node's failure got its
            // consumer skipped, which would otherwise excuse the failure.
            if early_stop && !self.nodes[node.0].self_cancelled {
                self.nodes[node.0].abandoned = true;
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
            match &state.truncated_by {
                Some(upstream) => Outcome::TruncatedInput {
                    upstream: upstream.clone(),
                },
                None => Outcome::Succeeded,
            }
        } else if state.abandoned {
            Outcome::Abandoned
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
