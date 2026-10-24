# Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Rust CLI that runs a DAG of programs, streaming stdout/stderr between them like non-linear UNIX pipes, with everything the graph allows running concurrently.

**Architecture:** A canonical `.proto` schema is compiled by `prost-build` into the one internal config model, with `pbjson-build` supplying serde impls so the same types load from JSON, TOML, or binary protobuf. Loading produces `pb::Graph`, which `Plan::from_proto` validates and resolves into an index-based execution plan. The executor is tokio: one task per node, one task per source stream that tees bytes to per-sink tasks, each sink gated by a byte-counting semaphore for backpressure. Outcomes collect into a `Report` that prints to stderr and maps to the process exit code.

**Tech Stack:** Rust edition 2024, tokio (multi-thread), prost + pbjson, serde_json + toml, clap, nix (signals/process groups), thiserror. `protoc` from nixpkgs at build time.

**Spec:** `docs/superpowers/specs/2026-10-24-orchestrator-design.md`

## Global Constraints

- Rust edition 2024, existing crate `orchestrator` v0.0.1. Binary name `orchestrator`.
- Target platforms: **Linux and macOS only**. UNIX process/signal/pipe semantics may be used directly; no Windows abstraction layer.
- `protoc` is required at build time: add `pkgs.protobuf` to both `nativeBuildInputs` of the `buildRustPackage` and the devShell `packages` in `flake.nix`.
- Exactly **one schema** (`proto/orchestrator/v1/graph.proto`) and **one internal config model** (the prost-generated types). No hand-maintained per-format structs.
- Default queue cap: **1 MiB per sink**. Default shutdown grace: **5000 ms**. Both overridable in config.
- Exit codes: **0** every node succeeded (or was abandoned, see Task 7); **1** any node failed, was skipped, or completed with truncated input; **2** config/validation error.
- The run summary and all diagnostics go to **stderr**. stdout carries only node pass-through output.
- Pass-through tagging is byte-oriented (`\n`-split, bytes untouched, never transcoded), whole-line atomic, on by default, disableable via config and `--no-tags`. Never auto-disabled based on TTY detection.
- Validation has **no filesystem side effects** — never create, open-for-write, or truncate an output path.
- `cargo fmt --check` and `cargo clippy -- -D warnings` must pass at every commit.

## Review Focus

Spec-implied conditions no task's happy path exercises; each has a test placed in the task that owns the code.

1. **A node that emits far more than the cap** while its consumer reads slowly — memory must stay bounded and no byte may be lost or reordered (Task 6).
2. **Non-UTF-8 node output through the tagger** — bytes must survive exactly, with the tag prepended per line (Task 3).
3. **A command that does not exist** — spawn failure is a node failure, not a crash, and its dependents are skipped (Task 7).
4. **A consumer that exits immediately** (`head -1`, `true`) — siblings still receive the full stream, and the producer dying of SIGPIPE once the *last* consumer is gone is not a graph failure (Tasks 6, 7).
5. **An unreadable file source and an unwritable output directory** — reported as validation errors (exit 2) before anything spawns, without creating or truncating any file (Tasks 2, 9).

---

### Task 1: Schema and config loading

**Files:**
- Create: `proto/orchestrator/v1/graph.proto`
- Create: `build.rs`
- Create: `src/pb.rs`
- Create: `src/config.rs`
- Modify: `src/main.rs`, `Cargo.toml`, `flake.nix`
- Test: `tests/config.rs`

**Interfaces:**
- Produces: `orchestrator::pb::{Graph, Node, Edge, DataEdge, OrderEdge, NodeStream, FileSink, Options, Stream}`; `orchestrator::config::{Format, load}` with `Format::from_path(&Path) -> Option<Format>` and `load(path: &Path, format: Option<Format>) -> Result<pb::Graph, ConfigError>`.

- [ ] **Step 1: Add dependencies and `protoc`**

`cargo add prost pbjson pbjson-types serde serde_json toml`, `cargo add --build prost-build pbjson-build`. In `flake.nix`, add `protobuf` to the package's `nativeBuildInputs` and to the devShell `packages`. Verify with `nix develop -c protoc --version`.

- [ ] **Step 2: Write the schema**

`proto/orchestrator/v1/graph.proto`, `syntax = "proto3"`, `package orchestrator.v1`:

```proto
message Graph {
  repeated Node nodes = 1;
  repeated Edge edges = 2;
  Options options = 3;
}

message Node {
  string name = 1;
  repeated string command = 2;        // argv; command[0] is the program, or the whole line when shell = true
  map<string, string> env = 3;
  bool clear_env = 4;                 // start from an empty environment instead of inheriting
  optional string working_dir = 5;
  bool shell = 6;                     // run `sh -c "<command joined by space>"`
  bool stdin_from_parent = 7;         // at most one node per graph
}

enum Stream {
  STREAM_UNSPECIFIED = 0;
  STREAM_STDOUT = 1;
  STREAM_STDERR = 2;
  STREAM_BOTH = 3;                    // stdout and stderr merged into one stream
}

message NodeStream { string node = 1; Stream stream = 2; }
message FileSink { string path = 1; bool append = 2; }

message DataEdge {
  oneof source { NodeStream node = 1; string file = 2; }
  oneof sink { string node_stdin = 3; FileSink file = 4; }
}

message OrderEdge {
  string after = 1;                   // upstream node that must exit first
  string node = 2;                    // downstream node that waits
  bool any_exit = 3;                  // false (default): upstream must exit 0
}

message Edge { oneof kind { DataEdge data = 1; OrderEdge order = 2; } }

message Options {
  optional uint64 queue_cap_bytes = 1;    // default 1048576
  bool no_tags = 2;
  optional uint64 shutdown_grace_ms = 3;  // default 5000
}
```

- [ ] **Step 3: Write `build.rs`**

Run `prost_build::Config` over the proto with `file_descriptor_set_path` set into `OUT_DIR`, then `pbjson_build::Builder` over that descriptor set to emit serde impls. Emit a `cargo:rerun-if-changed` for the proto.

- [ ] **Step 4: Write the failing test**

`tests/config.rs`:

```rust
const TOML_GRAPH: &str = r#"
[[nodes]]
name = "a"
command = ["printf", "hi"]

[[edges]]
[edges.data]
[edges.data.node]
node = "a"
stream = "STREAM_STDOUT"
[edges.data]
nodeStdin = "b"

[[nodes]]
name = "b"
command = ["cat"]
"#;

#[test]
fn toml_json_and_binary_agree() {
    // write the same graph as .toml, as .json (serde_json of the loaded graph),
    // and as .pb (prost encode), load all three, assert the three pb::Graph values are equal
}

#[test]
fn format_comes_from_extension_and_flag_overrides_it() {
    // Format::from_path("g.toml") == Some(Format::Toml), "g.json" => Json, "g.pb" => Binary,
    // "g.txt" => None; load(json_bytes_in_a_file_named_x, Some(Format::Json)) succeeds
}

#[test]
fn malformed_config_is_an_error_not_a_panic() {
    // load() on "nodes = 3" returns Err(ConfigError::Parse { .. })
}
```

Write the TOML sample as whatever shape pbjson's field naming actually accepts; the test is the contract, so settle the spelling here and reuse it in every later test. If pbjson's serde impls prove unusable for TOML specifically, fall back to `prost_build`'s `type_attribute` with plain `#[derive(serde::Deserialize)]` + `#[serde(rename_all = "camelCase")]` on the generated types, and keep JSON on the same impls; record which route was taken in a comment at the top of `build.rs`.

- [ ] **Step 5: Run test to verify it fails**

Run: `cargo test --test config`
Expected: FAIL — `orchestrator::config` does not exist.

- [ ] **Step 6: Implement**

`src/pb.rs` includes the generated module and the pbjson serde module. `src/config.rs` defines `enum Format { Json, Toml, Binary }`, `Format::from_path`, `ConfigError` (thiserror: `Io`, `Parse`, `UnknownFormat`), and `load`, dispatching to `serde_json`, `toml`, or `pb::Graph::decode`. `src/main.rs` declares `mod config; mod pb;` and keeps `fn main() {}` for now. Add `src/lib.rs` exposing the modules so integration tests can use them.

- [ ] **Step 7: Run tests**

Run: `cargo test --test config && cargo clippy -- -D warnings && cargo fmt --check`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add proto build.rs src Cargo.toml Cargo.lock flake.nix tests
git commit -m "feat: canonical proto schema and multi-format config loading"
```

---

### Task 2: Resolved plan and validation

**Files:**
- Create: `src/graph.rs`
- Test: `tests/validate.rs`

**Interfaces:**
- Consumes: `pb::Graph` from Task 1.
- Produces:

```rust
pub struct NodeId(pub usize);
pub enum StdinSource { Empty, Parent, Node { from: NodeId, stream: pb::Stream }, File(PathBuf) }
pub struct OrderDep { pub after: NodeId, pub any_exit: bool }
pub enum SinkSpec { NodeStdin(NodeId), File { path: PathBuf, append: bool }, Passthrough(pb::Stream) }
pub struct NodeSpec {
    pub name: String, pub command: Vec<String>, pub env: BTreeMap<String, String>,
    pub clear_env: bool, pub working_dir: Option<PathBuf>, pub shell: bool,
    pub stdin: StdinSource, pub order_deps: Vec<OrderDep>,
}
pub struct Fanout { pub source: NodeId, pub stream: pb::Stream, pub sinks: Vec<SinkSpec> }
pub struct Plan {
    pub nodes: Vec<NodeSpec>, pub fanouts: Vec<Fanout>,
    pub queue_cap_bytes: usize, pub tags: bool, pub shutdown_grace: Duration,
}
impl Plan {
    pub fn from_proto(g: pb::Graph) -> Result<Plan, Vec<ValidationError>>;
    pub fn node(&self, id: NodeId) -> &NodeSpec;
    pub fn to_dot(&self) -> String;
}
pub enum ValidationError { /* one variant per rule below, each carrying the offending names */ }
```

- [ ] **Step 1: Write the failing tests**

`tests/validate.rs` — one test per rule, each asserting the specific `ValidationError` variant:

```rust
#[test] fn rejects_cycles()                      // a -> b -> a across mixed data and order edges
#[test] fn rejects_two_inbound_data_edges()      // two data edges into c's stdin
#[test] fn rejects_duplicate_node_names()
#[test] fn rejects_edge_to_unknown_node()
#[test] fn rejects_both_alongside_stdout_source()  // `a` has a STREAM_BOTH edge and a STREAM_STDOUT edge
#[test] fn rejects_two_nodes_reading_parent_stdin()
#[test] fn rejects_empty_command()
#[test] fn rejects_unreadable_file_source()      // source path does not exist
#[test] fn rejects_output_dir_missing_or_unwritable()
#[test] fn rejects_two_file_sinks_on_one_path()
#[test] fn rejects_sink_path_also_used_as_source()
#[test] fn validation_creates_no_files()         // a graph whose only sink is tmp/out.txt; after
                                                 // from_proto returns Ok, out.txt must not exist
#[test] fn unredirected_streams_become_passthrough_sinks()
         // node with no data edge on stdout gets Fanout { stream: STDOUT, sinks: [Passthrough(STDOUT)] };
         // a node whose stdout IS redirected gets no passthrough sink for stdout but still one for stderr
#[test] fn stream_both_covers_both_streams()
         // a STREAM_BOTH data edge leaves neither stdout nor stderr with a passthrough sink
#[test] fn defaults_applied()                    // absent options => 1 MiB cap, tags on, 5s grace
#[test] fn diamond_resolves()                    // the spec's A->{B,C,D}->E graph: E.stdin is Node{C,STDOUT},
                                                 // E.order_deps == [b, d], A has one fanout with three sinks
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test validate`
Expected: FAIL — `Plan` does not exist.

- [ ] **Step 3: Implement `Plan::from_proto`**

Resolve names to `NodeId` first, then run every rule and collect **all** errors before returning (an operator fixing a graph wants the whole list). Cycle detection over the union of data and order edges; a DFS with a colour map or Kahn's algorithm, either is fine. Group data edges by source into `Fanout`s. For each node stream not covered by any data edge, append a `Passthrough` sink. For output-path checks, stat the parent directory — never open the file.

- [ ] **Step 4: Implement `Plan::to_dot`**

Digraph with one node per `NodeSpec` labelled by name; solid edges labelled with the stream for data edges, dashed for order edges; file sources and sinks as box-shaped nodes labelled with the path.

- [ ] **Step 5: Run tests**

Run: `cargo test --test validate && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/graph.rs tests/validate.rs src/lib.rs
git commit -m "feat: resolve and validate graphs into an execution plan"
```

---

### Task 3: Line-tagging writer

**Files:**
- Create: `src/exec/tag.rs`, `src/exec/mod.rs`
- Test: unit tests in `src/exec/tag.rs`

**Interfaces:**
- Produces: `pub struct TaggedWriter` with
  `TaggedWriter::new(tag: Option<String>, out: Arc<Mutex<Box<dyn Write + Send>>>) -> Self`,
  `async fn write_chunk(&mut self, bytes: &[u8]) -> io::Result<()>`,
  `async fn flush_partial(&mut self) -> io::Result<()>`.
  Shared stdout/stderr handles live behind the `Arc<Mutex<..>>` so whole-line writes are atomic across nodes.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn tags_each_line_with_the_node_name()
    // chunks "he", "llo\nworld\n" => b"[a] hello\n[a] world\n"
#[tokio::test] async fn flushes_a_final_line_without_a_newline()
    // "tail" then flush_partial => b"[a] tail\n"
#[tokio::test] async fn passes_non_utf8_bytes_through_untouched()
    // b"\xff\xfe\n" => b"[a] \xff\xfe\n"  (Review Focus 2)
#[tokio::test] async fn no_tag_means_raw_bytes_and_no_line_splitting()
    // TaggedWriter::new(None, ..) with "a\nb" => b"a\nb", flush_partial adds nothing
#[tokio::test] async fn two_writers_never_interleave_within_a_line()
    // two writers on one shared sink, each writing many lines concurrently;
    // every line in the output is wholly one tag's
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test tag::`
Expected: FAIL — `TaggedWriter` not found.

- [ ] **Step 3: Implement `TaggedWriter`**

Hold a partial-line buffer; on each chunk, split on `\n`, and write `[tag] <line>\n` for every complete line in one `lock()`-and-write so a line cannot be split by another writer. With `tag: None`, write the chunk straight through with no buffering.

- [ ] **Step 4: Run tests**

Run: `cargo test tag:: && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/exec
git commit -m "feat: byte-oriented line tagging for pass-through output"
```

---

### Task 4: Spawning a node

**Files:**
- Create: `src/exec/child.rs`
- Modify: `src/exec/mod.rs`, `Cargo.toml`
- Test: `tests/child.rs`

**Interfaces:**
- Consumes: `NodeSpec`, `StdinSource` from Task 2.
- Produces:

```rust
pub struct Spawned { pub child: tokio::process::Child, pub pgid: i32 }
pub fn spawn(spec: &NodeSpec, stdin: Stdio, want_stdout: bool, want_stderr: bool, merged: bool) -> io::Result<Spawned>;
```
`merged = true` means stdout and stderr must arrive on one stream, so both child fds point at the same pipe write end and only `child.stdout` is read.

- [ ] **Step 1: Add dependencies**

`cargo add tokio --features full`, `cargo add nix --features signal,process`, `cargo add --dev tempfile`.

- [ ] **Step 2: Write the failing tests**

`tests/child.rs`:

```rust
#[tokio::test] async fn runs_argv_and_captures_stdout()
#[tokio::test] async fn shell_true_runs_the_command_line_through_sh()     // ["echo $HOME"] with shell
#[tokio::test] async fn env_overrides_are_applied_and_parent_env_inherited()
#[tokio::test] async fn clear_env_starts_empty()                          // `env` prints only the overrides
#[tokio::test] async fn working_dir_is_respected()                        // `pwd` in a tempdir
#[tokio::test] async fn merged_puts_stderr_on_the_stdout_pipe()           // sh -c 'echo o; echo e >&2'
#[tokio::test] async fn child_is_in_its_own_process_group()               // pgid != orchestrator's pgid
#[tokio::test] async fn missing_program_is_an_io_error_not_a_panic()       // Review Focus 3
#[tokio::test] async fn stdin_source_parent_inherits_the_fd()
    // StdinSource::Parent => Stdio::inherit(), not a forwarding pipe, so TTY-ness survives;
    // assert the child's stdin fd is the orchestrator's (compare `stat -L /dev/stdin` style
    // identity, or that `Stdio` is inherit at the call site)
#[tokio::test] async fn stdin_source_empty_gives_immediate_eof()           // `cat` exits with no output
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --test child`
Expected: FAIL — `spawn` not found.

- [ ] **Step 4: Implement `spawn`**

`tokio::process::Command`, `process_group(0)` for the own-group requirement, `env_clear()` when `clear_env`, `current_dir` when set, `sh -c` with the command joined by spaces when `shell`. For `merged`, create one `os_pipe`-style pair (or `nix::unistd::pipe`) and hand duplicates of the write end to both `stdout` and `stderr`, returning the read end as the child's stdout. Return the real pgid via `nix::unistd::getpgid`.

- [ ] **Step 5: Run tests**

Run: `cargo test --test child && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/exec/child.rs src/exec/mod.rs Cargo.toml Cargo.lock tests/child.rs
git commit -m "feat: spawn nodes with env, cwd, shell, merged streams, and own process group"
```

---

### Task 5: Sinks

**Files:**
- Create: `src/exec/sinks.rs`
- Modify: `src/exec/mod.rs`
- Test: unit tests in `src/exec/sinks.rs`

**Interfaces:**
- Consumes: `TaggedWriter` from Task 3, `SinkSpec` from Task 2, `Spawned` from Task 4.
- Produces:

```rust
pub struct ChildStdinSink(Option<tokio::process::ChildStdin>);
pub struct FileByteSink(tokio::fs::File);          // FileByteSink::open(path: &Path, append: bool)
pub struct PassthroughSink(TaggedWriter);
/// A concrete enum rather than a trait object: the sink set is closed, and
/// `dyn` + `async fn` would force boxed futures for no benefit.
pub enum Sink {
    ChildStdin(ChildStdinSink), File(FileByteSink), Passthrough(PassthroughSink),
    #[cfg(any(test, feature = "test-sinks"))] Test(TestSink),  // scripted delay/error, for Task 6
}
impl Sink {
    pub async fn write_all(&mut self, chunk: &[u8]) -> io::Result<()>;
    pub async fn finish(self);
}
pub struct SinkCtx { /* child stdin handles by NodeId, shared tagged stdout/stderr writers */ }
pub fn build_sink(spec: &SinkSpec, ctx: &mut SinkCtx) -> io::Result<Sink>;
```

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn file_sink_truncates_by_default()      // pre-existing content gone
#[tokio::test] async fn file_sink_appends_when_asked()
#[tokio::test] async fn child_stdin_sink_closes_the_pipe_on_finish()      // `cat` sees EOF and exits
#[tokio::test] async fn child_stdin_sink_reports_broken_pipe_as_closed()  // consumer already exited
#[tokio::test] async fn passthrough_sink_tags_and_flushes_a_partial_line()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test sinks::`
Expected: FAIL.

- [ ] **Step 3: Implement the sinks**

`finish()` must drop the child stdin handle (so the consumer sees EOF) and call `flush_partial` for the passthrough sink. `write_all` returns the `io::Error` as-is; classifying `ErrorKind::BrokenPipe` as a clean close is the tee's job in Task 6. `TestSink` exists so Task 6 can script slow and failing consumers without spawning processes.

- [ ] **Step 4: Run tests**

Run: `cargo test sinks:: && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/exec/sinks.rs src/exec/mod.rs
git commit -m "feat: file, child-stdin, and pass-through sinks"
```

---

### Task 6: Tee with bounded per-sink queues

**Files:**
- Create: `src/exec/tee.rs`
- Modify: `src/exec/mod.rs`, `Cargo.toml`
- Test: unit tests in `src/exec/tee.rs`

**Interfaces:**
- Consumes: `Sink` (including `Sink::Test`) from Task 5.
- Produces:

```rust
pub enum SinkOutcome { Finished, Closed, Cancelled, Errored(io::Error) }
pub struct Tee { /* .. */ }
impl Tee {
    pub fn new(cap_bytes: usize) -> Self;
    pub fn add_sink(&mut self, name: String, sink: Sink) -> CancellationToken;
    /// Reads `src` to EOF or until every sink is gone, then drops `src`.
    /// Returns true if it stopped early because no sinks remained.
    pub async fn run(self, src: impl AsyncRead + Unpin) -> (bool, Vec<(String, SinkOutcome)>);
}
```

- [ ] **Step 1: Add dependency**

`cargo add tokio-util --features rt` for `CancellationToken`, `cargo add bytes`.

- [ ] **Step 2: Write the failing tests**

```rust
#[tokio::test] async fn every_sink_gets_every_byte()
#[tokio::test] async fn a_closed_sink_is_dropped_and_the_others_continue()
    // one sink returns BrokenPipe after 1 chunk; the other receives the whole input;
    // outcomes are [Closed, Finished]  (Review Focus 4)
#[tokio::test] async fn a_write_error_that_is_not_broken_pipe_is_reported_as_errored()
#[tokio::test] async fn a_cancelled_sink_releases_its_backpressure()
    // cap = 64; a sink that never reads is cancelled mid-run; run() still completes
    // and the fast sink gets everything  (the deadlock the spec calls out)
#[tokio::test] async fn a_slow_sink_does_not_hold_back_a_fast_one()
    // fast sink observes chunk N while the slow sink is still on chunk 0
#[tokio::test] async fn run_reports_early_stop_when_the_last_sink_goes()
    // single sink closes immediately => run() returns (true, ..) without draining src
#[tokio::test] async fn memory_stays_bounded_and_no_bytes_are_lost()
    // cap = 4096, 8 MiB of input, a sink that reads with a delay;
    // assert the sink received all 8 MiB in order and in-flight bytes never exceeded
    // cap + one chunk  (Review Focus 1)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test tee::`
Expected: FAIL.

- [ ] **Step 4: Implement `Tee`**

One task per sink reading an unbounded `mpsc` of `Bytes`, with byte-level backpressure from a per-sink `Arc<Semaphore>` holding `cap_bytes` permits: the reader acquires `chunk.len()` permits (clamped to the cap) before enqueueing, the sink task releases them after the write returns. The read loop writes each chunk to every live sink; a sink whose write errored, whose channel closed, or whose token was cancelled is removed from the live set and must have its permits released and its semaphore closed so the reader can never block on a gone sink. `ErrorKind::BrokenPipe` becomes `Closed`, any other error `Errored`. When the live set empties, return `(true, outcomes)` and drop `src` — that closes the orchestrator's read end so the producer takes EPIPE/SIGPIPE.

- [ ] **Step 5: Run tests**

Run: `cargo test tee:: && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/exec/tee.rs src/exec/mod.rs Cargo.toml Cargo.lock
git commit -m "feat: byte-bounded tee with sink cancellation"
```

---

### Task 7: Scheduler, failure propagation, and report

**Files:**
- Create: `src/exec/run.rs`, `src/report.rs`
- Modify: `src/exec/mod.rs`
- Test: `tests/run.rs`, `tests/common/mod.rs`

**Interfaces:**
- Consumes: Tasks 2–6 (`Plan`, `TaggedWriter`, `spawn`, `build_sink`, `Tee`).
- Produces:

```rust
pub enum Outcome {
    Succeeded,
    Abandoned,                                  // died after its last consumer went away
    TruncatedInput { upstream: String },        // exited 0 but its stdin was cut by an upstream failure
    Failed { code: Option<i32>, signal: Option<i32> },
    SpawnFailed { message: String },
    Skipped { because: String },                // name of the node whose failure caused the skip
}
pub struct Report { pub entries: Vec<(String, Outcome)> }
impl Report {
    pub fn write(&self, w: &mut impl io::Write) -> io::Result<()>;
    pub fn exit_code(&self) -> i32;   // 0 iff every outcome is Succeeded or Abandoned, else 1
}
pub async fn run(plan: &Plan) -> Report;
```

- [ ] **Step 1: Write the failing report unit tests**

In `src/report.rs`:

```rust
#[test] fn exit_code_is_zero_for_success_and_abandonment()
#[test] fn exit_code_is_one_for_failure_skip_or_truncation()
#[test] fn summary_names_the_cause_of_each_skip()
```

- [ ] **Step 2: Write the failing integration tests**

`tests/common/mod.rs` provides `run_graph(toml: &str) -> std::process::Output`, writing the graph to a tempdir and invoking `env!("CARGO_BIN_EXE_orchestrator")` with `run`.

`tests/run.rs`:

```rust
#[test] fn parallel_nodes_run_in_parallel()
    // three independent `sleep 1` nodes; elapsed < 2s   (the spec's first anti-goal)
#[test] fn downstream_streams_before_upstream_exits()
    // a: sh -c 'printf early; sleep 3' -> b: sh -c 'cat > out; ...' observing "early"
    // written to a file within 1s, well before a exits
#[test] fn diamond_end_to_end()
    // the spec's example: A -> B,C,D (data), E data from C, order edges from B and D;
    // E's output proves it got C's bytes, and B/D/C all ran concurrently
#[test] fn order_edge_blocks_a_node_whose_data_is_already_flowing()
    // E must not start until B exits, even though C's bytes are queued; assert via
    // timestamps written by each node
#[test] fn unrelated_branches_complete_when_one_node_fails()
    // the user's core requirement: `false` fails, the parallel branch still writes its file
#[test] fn unstarted_dependents_are_skipped_not_run_empty()
    // order-edge dependent of a failed node never executes; its output file does not exist;
    // summary says Skipped, exit code 1
#[test] fn skipping_propagates_transitively()
#[test] fn any_exit_order_edge_runs_after_a_failure()
    // the cleanup-node case the user chose
#[test] fn running_dependent_finishes_with_truncated_input()
    // producer prints some bytes then exits 1; the already-running consumer exits 0;
    // its outcome is TruncatedInput and the run's exit code is 1
#[test] fn missing_program_is_reported_as_spawn_failure()   // Review Focus 3
#[test] fn consumer_that_exits_early_does_not_disturb_siblings()
    // A -> {head -1, cat > out}; out gets the whole stream           (Review Focus 4)
#[test] fn producer_killed_by_sigpipe_is_abandoned_not_failed()
    // `yes` -> single `head -1` consumer; exit code 0, outcome Abandoned  (Review Focus 4)
#[test] fn unredirected_output_is_tagged_on_the_right_stream()
    // sh -c 'echo o; echo e >&2' => stdout has "[n] o", stderr has "[n] e" and the summary
#[test] fn summary_goes_to_stderr_and_stdout_stays_clean()
#[test] fn empty_graph_succeeds_with_exit_zero()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --test run`
Expected: FAIL — `run` not implemented.

- [ ] **Step 4: Implement `run`**

Spawn-readiness loop: a node is ready when every `OrderDep` is satisfied (upstream exited, and exited 0 unless `any_exit`) and, if its stdin comes from a node, that node has been *started*. Spawn every ready node immediately — no stage barrier. For each `Fanout`, start its tee task as soon as the source node is spawned; sinks for node-stdin destinations need that consumer's stdin handle, so a consumer blocked by an order edge gets its sink attached only once it spawns, which is exactly what makes the queue fill.

Failure propagation: when a node fails, walk its dependents; mark each unstarted one `Skipped { because }` and cancel the sink token for its queued stdin (releasing the producer's backpressure per Task 6); mark each already-started data dependent `TruncatedInput` if it ultimately exits 0. A node whose tee reported early stop (no sinks left) and which then exits nonzero or by SIGPIPE is `Abandoned`.

Print the report to stderr; `main` returns `report.exit_code()`.

- [ ] **Step 5: Run tests**

Run: `cargo test && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/exec/run.rs src/report.rs src/exec/mod.rs tests
git commit -m "feat: concurrent scheduler with failure propagation and run report"
```

---

### Task 8: Signals and child lifetime

**Files:**
- Create: `src/exec/signals.rs`
- Modify: `src/exec/run.rs`
- Test: `tests/signals.rs`

**Interfaces:**
- Produces: `pub async fn shutdown(children: &[(String, i32)], grace: Duration)` — SIGTERM each pgid, wait up to `grace` for exits, then SIGKILL the remainder; and a `tokio::signal` watcher wired into `run`'s select loop.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn sigterm_stops_scheduling_and_terminates_children()
    // graph of long sleeps; send SIGTERM to the orchestrator; it exits nonzero within
    // grace + margin and no `sleep` child survives
#[test] fn a_child_that_ignores_sigterm_is_killed_after_the_grace_period()
    // sh -c 'trap "" TERM; sleep 60' with shutdown_grace_ms = 500; orchestrator exits,
    // child is gone
#[test] fn no_orphans_on_a_normal_run()
    // after a clean run, no child pids remain
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test signals`
Expected: FAIL.

- [ ] **Step 3: Implement**

`nix::sys::signal::killpg` on each live child's pgid. On signal: stop spawning new nodes, run `shutdown`, print the report, exit nonzero. Always reap children on the normal path too.

- [ ] **Step 4: Run tests**

Run: `cargo test --test signals && cargo clippy -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/exec/signals.rs src/exec/run.rs tests/signals.rs
git commit -m "feat: signal forwarding with grace period and no orphaned children"
```

---

### Task 9: CLI

**Files:**
- Create: `src/cli.rs`
- Modify: `src/main.rs`, `Cargo.toml`
- Test: `tests/cli.rs`

**Interfaces:**
- Consumes: Tasks 1, 2, 7.
- Produces: `orchestrator run <graph> [--format json|toml|pb] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]`, `orchestrator validate <graph> [--format ..]`, `orchestrator dot <graph> [--format ..]`. CLI flags override config `Options`.

- [ ] **Step 1: Add dependency**

`cargo add clap --features derive`.

- [ ] **Step 2: Write the failing tests**

```rust
#[test] fn validate_prints_the_resolved_plan_and_exits_zero()
#[test] fn validate_reports_every_error_at_once_and_exits_two()
    // a graph with a cycle and an unknown node: both named in stderr, exit 2
#[test] fn validate_does_not_create_or_truncate_output_files()   // Review Focus 5
#[test] fn validate_rejects_a_missing_file_source_with_exit_two() // Review Focus 5
#[test] fn dot_emits_a_digraph_naming_every_node()
#[test] fn no_tags_flag_produces_raw_output()
#[test] fn queue_cap_flag_overrides_the_config_value()
#[test] fn bad_config_path_exits_two()
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --test cli`
Expected: FAIL.

- [ ] **Step 4: Implement**

clap derive in `src/cli.rs`; `main` loads the config, applies flag overrides onto `Options` before `Plan::from_proto`, dispatches the subcommand, and returns 2 for `ConfigError`/`ValidationError`, otherwise the report's code.

- [ ] **Step 5: Run the whole suite**

Run: `cargo test && cargo clippy -- -D warnings && cargo fmt --check`
Expected: PASS.

- [ ] **Step 6: Write the README usage section**

`README.md`: the diamond example as a TOML graph, the three subcommands, the exit codes, and the stdout/stderr contract.

- [ ] **Step 7: Commit**

```bash
git add src/cli.rs src/main.rs README.md Cargo.toml Cargo.lock tests/cli.rs
git commit -m "feat: run, validate, and dot subcommands"
```
