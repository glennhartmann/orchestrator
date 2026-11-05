# Orchestrator — Design

Date: 2026-10-24
Status: approved design, pending plan

## Intent

`orchestrator` is a Rust CLI that runs a graph of programs the way a shell
runs a pipeline, except the pipeline is not a line. One program's output can
feed several programs and several files at once; a program can take its input
from another program or from a file; and everything the graph structure allows
to run at the same time does run at the same time.

The why: shell pipelines only express a chain. Expressing a real workflow — a
dependency fan-out, a couple of long-running remote jobs whose results feed a
final stage — means either a hand-written script full of `&`, `wait`, and
temporary files, or a heavyweight workflow engine. This tool is the small,
sharp thing in between: write the graph down once, launch it, walk away, come
back to the output.

The nodes are ordinary UNIX programs. The interesting case, and the one that
drove this, is **long-running programs that move modest amounts of data** —
remote workflow drivers that take minutes and emit kilobytes. So the value is
in the orchestration and the wiring, not in throughput. That said, IO is
genuinely streamed (see Streaming), not staged.

## Goals

- Express a DAG of programs in a single static config file.
- Run every node that can run concurrently, concurrently.
- Stream data between nodes as it is produced, like a UNIX pipe.
- Fan one node's output out to multiple programs and multiple files at once.
- Forward stdout and stderr separately, or merged into one stream.
- Take node input from another node's output or from a file.
- Survive partial failure: a failing node must not stop unrelated work.
- Leave the operator with readable output when they come back to the terminal.
- One schema, defined in proto; graphs authored as JSON or TOML and also
  accepted as binary protobuf. Protobuf *text* format is explicitly not
  required (user's call) — JSON and TOML are the hand-authored formats.

## Non-goals

- Per-node timeouts and retries. Explicitly out of scope (user's call).
- Launch-time parameterization or templating of graphs. Graphs are fully
  static files (user's call).
- Distributed execution, scheduling across machines, containers, resource
  limits, cgroups.
- Large-data throughput engineering. Correct streaming is required; squeezing
  gigabytes per second is not.
- A DSL, a UI, or a daemon. It is a one-shot CLI.
- Caching, resumption, or incremental re-execution.

## Anti-goals

Things that would make this a failure even if it passed its tests:

1. **Accidental serialization.** This is the big one. If nodes that could run
   in parallel end up waiting on each other — because of a lock, a mutex
   around the whole tee, a single-threaded reader, an eager "collect upstream
   output then start downstream" shortcut, or an order edge inferred where the
   user only asked for data — the tool has failed at its main job. Parallelism
   that the graph permits must be observable in wall-clock time.
2. **A slow or dead consumer poisoning the fan-out.** One downstream program
   that stops reading must not stall its siblings.
3. **Unbounded memory growth** from a node that unexpectedly spews output.
4. **Illegible interleaved output** when several nodes write to the terminal
   at once.

## Execution model

### Nodes

A node is one program execution. Each node has:

- a unique **name** (referenced by edges and used in output tags)
- a **command** and **args**
- optional **environment variables** (and whether to inherit the parent env —
  builder's call on the default; recommend inherit, with per-node overrides
  merged on top, and an explicit "clear" option)
- optional **working directory**
- optional **shell** execution: run the command line through a shell rather
  than exec'ing it directly

### Edges

Exactly two kinds of edge, and the distinction matters:

**Data edge.** Carries bytes from a source to a sink.

- Source: a node's `stdout`, its `stderr`, or `both` (the two merged into one
  stream); or a file.
- Sink: a node's `stdin`, or a file.
- **A node has at most one inbound data edge.** There is no merging of
  multiple inputs into one stdin. (The user corrected an earlier sketch here:
  no multi-input nodes.)
- A single source may have many outbound data edges — any mix of nodes and
  files. That is the fan-out.

**Order edge.** Carries no data. `B -> C` as an order edge means: `C` does not
start until `B` has exited.

- By default the upstream must exit **successfully**; if it fails, the
  downstream node is skipped. A per-edge schema field relaxes this to "any
  exit", which is what makes cleanup/finalizer nodes expressible (user's
  call: per-edge, defaulting to success).
- An order edge **does** block a node that already has data waiting on its
  data edge. If `E` takes data from `C` but has an order edge from `B`, then
  `E` does not start until `B` exits, and `C`'s output to `E` accumulates in
  the bounded buffer meanwhile (throttling `C` if the cap is reached). The
  user confirmed this explicitly.

A node with no inbound data edge gets an empty stdin (immediate EOF) unless it
is the one node designated to read the orchestrator's own stdin (my call —
see Open calls).

### Scheduling

A node becomes **runnable** when every order-edge predecessor has exited and
satisfied that edge's status requirement (success by default, any exit if the
edge says so), and its data edge's source node has been *started* (not
finished). Runnable nodes are spawned immediately and concurrently. There is
no wave or stage model — the implementation must never wait for a "layer" to
complete.

In the canonical example (`A` fans out to `B`, `C`, `D` via data edges; `E`
takes data from one of them and order edges from the others), `A`, `B`, `C`,
and `D` are all spawned at once and stream; `E` waits only on its order-edge
predecessors.

### Streaming

Data edges are real pipes, not staging buffers. `B` starts as soon as `A`
starts and consumes `A`'s bytes as they arrive.

Fan-out is a **tee**: one reader on the source's pipe, writing each chunk to
every destination (node stdins and files) concurrently.

- **A consumer that closes its stdin or exits** is dropped from the tee. The
  producer and the remaining consumers continue undisturbed. A broken-pipe
  error on one destination is not an error for the graph (it is the normal
  `head`-in-a-pipeline case).
- **When the last destination of a stream drops, the orchestrator closes its
  read end of that stream**, so the producer gets EPIPE/SIGPIPE and
  terminates — the same way `yes | head -1` ends `yes`. Without this a
  `yes`-style producer would run forever writing into nothing. A producer
  that dies this way is *not* counted as a graph failure.
- **A destination that will never read** — a node that got skipped while its
  queue was filling — is dropped from the tee at the moment it is skipped,
  releasing any backpressure it was applying. The tee's sinks therefore need
  an explicit "cancelled" path alongside "closed" and "errored"; without it,
  a skipped node that was blocking a throttled producer would deadlock the
  run.
- **A slow consumer** gets its own bounded queue, so it cannot hold back the
  fast consumers: the fast ones are written to directly while the slow one
  drains its queue.
- **The queue has a size cap** with a sensible default, overridable in config
  (globally, and per-edge if cheap to offer). When a consumer's queue is full,
  the producer is throttled — backpressure, exactly as a full pipe buffer
  would behave. Nothing is dropped; the data is never silently lost. Default:
  builder's call, with 1 MiB per edge suggested as a starting point given the
  kilobyte-scale expectation.

### Failure

A node **fails** if it cannot be spawned or exits nonzero.

- Anything not reachable from the failed node through the dependency relation
  **runs to completion**, untouched. This is the user's explicit requirement.
- A dependent that **has not started** is **skipped** — not run with empty or
  partial input. Skipping propagates transitively through both data and order
  edges.
- A dependent that **has already started** (true for data-edge dependents,
  which start as soon as the producer starts) keeps running. Its stdin sees
  EOF where the stream was cut. It is not killed. Its own exit status is
  reported as its own, and the run summary records that its input was
  truncated by an upstream failure. This follows from streaming: by the time
  the upstream failure is known, downstream has already consumed bytes.
- The orchestrator always drains to quiescence: it never leaves orphaned child
  processes behind when it exits.

### Output and observability

- Any node stdout or stderr **not** redirected by a data edge passes through
  to the orchestrator's own stdout/stderr — a node's stdout to the
  orchestrator's stdout, its stderr to the orchestrator's stderr.
- Pass-through output is **tagged per line with the node name**, so parallel
  output stays readable. A final partial line with no trailing newline is
  still flushed and tagged. Tag format is the builder's call; `[name] text` is
  the obvious one. Writes to a shared stream must be whole-line atomic so two
  nodes cannot interleave mid-line.
- Line splitting is **byte-oriented** (split on `\n`, pass the bytes through
  untouched). Node output is not required to be UTF-8 and must never be
  lossily transcoded.
- Tagging can be **turned off** — globally via config and CLI flag, so the
  orchestrator's stdout can carry a node's raw bytes when it is itself being
  piped somewhere. Tagging does *not* silently switch itself off based on
  whether stdout is a TTY; the behavior is explicit.
- On completion the orchestrator prints a **summary to stderr** (never
  stdout, which belongs to the nodes): per node, whether it succeeded, failed
  (with exit status), was skipped (and because of which failure), or
  completed with truncated input.
- **Exit code**: 0 if every node ran and succeeded. 1 if any node failed, was
  skipped, or completed with truncated input — a truncated-input node counts
  as a failed *run* even though the node exited 0, because the run's results
  are wrong. 2 for config/validation errors, so scripts can tell "I wrote a
  bad graph" from "the graph ran and something in it broke".

### Signals and child lifetime

- Each child is spawned in **its own process group**, so the orchestrator
  controls signal delivery rather than the terminal broadcasting to everyone.
- On SIGINT or SIGTERM to the orchestrator: forward the signal to every live
  child, stop scheduling new nodes, wait a grace period (builder's call;
  5–10s suggested), then SIGKILL whatever is left and exit nonzero. A child
  that ignores signals must not be able to hang the orchestrator forever.
- Normal exit path reaps every child. No orphans, ever.

## Configuration

One canonical schema, defined as a **`.proto`**. A graph is authored as JSON
or TOML, and also accepted as binary protobuf; all three deserialize to the
same internal model. Format is picked from the file extension, with a CLI flag
to override.

Validation happens before anything is spawned, and rejects:

- cycles in the combined data+order dependency graph
- a data edge `X -> Y` where `Y` has an order-dependency path back to `X`.
  Not a cycle — both kinds of edge run `X -> Y` — but exactly the deadlock
  condition: `Y` cannot start until `X` exits, while `X` throttles on `Y`'s
  unread queue. (Added during implementation; the original list missed it.)
- more than one inbound data edge on a node
- duplicate node names
- edges referring to unknown nodes
- a `both` (merged) source edge on the same node as a separate `stdout` or
  `stderr` source edge — `both` is mutually exclusive with the individual
  streams, rather than meaning "also tee a merged copy"
- more than one node designated to read the orchestrator's stdin
- unreadable input files
- output paths whose parent directory is missing or not writable
- two file sinks writing the same path, or a file sink that truncates a path
  another node reads as a file source — both are nondeterministic races

**Validation has no filesystem side effects.** It must not create or truncate
output files, which rules out "try opening it": check the parent directory
instead, and accept that this is a weaker check than a real open.

File destinations **truncate by default, with append available** (my call,
offered to the user and not objected to). File sources are read and streamed
like any other data source.

CLI surface (builder's call on exact spelling): a `run` subcommand, a
`validate`/dry-run subcommand that does the full validation and prints the
resolved plan without executing, and — cheap and useful — a way to emit the
graph as Graphviz DOT for eyeballing.

## Implementation notes

Rust, edition 2024, in the existing empty `orchestrator` crate. Target
platforms are **Linux and macOS**; Windows is out of scope, so UNIX process,
signal, and pipe semantics may be relied on directly rather than abstracted.
The rest are recommendations, not requirements; the builder owns them:

- **Async runtime: tokio**, multi-threaded. Process spawning, pipe IO, and
  per-node tasks all map cleanly onto it, and it makes "everything runs at
  once" the path of least resistance.
- **Proto: prost** (+ `prost-build`) for the canonical schema, with JSON
  mapping via `pbjson` or by deserializing JSON/TOML through `serde` into the
  same generated types. Whichever route, there must be exactly one schema and
  one internal model — no parallel hand-maintained structs per format.
- One tokio task per node; one task per source stream doing the tee; bounded
  `mpsc` channels as the per-consumer queues (bounded channels give the
  backpressure requirement for free).
- Nothing in the data path should take a lock that spans more than one chunk.

### Testing

The anti-goals are the test plan:

- A graph whose nodes `sleep`, asserting total wall-clock proves they ran in
  parallel, not in sequence.
- A streaming test: a slow producer and a downstream node that must be
  observed emitting output *before* the producer exits.
- A consumer that exits immediately (`head -1`) while siblings must still
  receive the full stream.
- A slow consumer that must not slow down a fast sibling.
- A high-volume producer against a small cap, asserting bounded memory and
  that no bytes are lost.
- A mid-graph failure, asserting unrelated branches complete, unstarted
  dependents are skipped, and the exit code and summary are right.
- An order edge blocking a node whose data is already flowing.

## Open calls I made

Flagged so they can be checked while reading, and reversed cheaply:

1. Unstarted dependents of a failed node are **skipped**, while already-running
   data-edge dependents are **allowed to finish with truncated stdin** rather
   than being killed.
2. One node may optionally be designated to read the orchestrator's own
   **stdin**, for pipeline parity. At most one, and it **inherits the fd
   directly** rather than being fed through a forwarding pipe — that
   preserves TTY-ness, so a node that prompts for a credential still works.
   It is not part of the tee machinery.
3. File sinks **truncate by default**, append on request.
4. Pass-through tagging is **per line**, byte-oriented, with whole-line atomic
   writes, and explicitly disableable.
5. A `validate`/dry-run mode and DOT output are in scope as small additions,
   and validate has no filesystem side effects.
6. Closing the read end when the last consumer drops, so producers get
   SIGPIPE — the `yes | head -1` behavior.
7. A skipped node's queue is dropped from the tee, releasing backpressure.
8. Exit codes: 0 clean, 1 node failure/skip/truncation, 2 config error.
   Truncated-input nodes make the run nonzero.
9. Per-child process groups, signal forwarding with a grace period then
   SIGKILL.
10. Linux and macOS only.
11. Validation rejects `both` alongside `stdout`/`stderr` on the same node,
    and rejects file-path aliasing between sinks, or between a sink and a
    source.
12. The run summary goes to **stderr**, keeping stdout clean for node output.
