# orchestrator

Runs a DAG of programs, streaming each node's stdout/stderr to the
destinations named in a graph config: another node's stdin, a file, or
(when nothing claims a stream) straight through to the orchestrator's own
stdout/stderr. Linux and macOS.

A shell pipeline is a line. This is not: one node's output can feed several
programs and several files at once, and everything the graph allows to run
concurrently does.

## Building a graph

A graph is a list of nodes plus a list of edges, loaded from TOML, JSON, or
prost's binary wire format (`.toml`, `.json`, `.pb`; override with
`--format`). Field names follow pbjson's camelCase convention, and `Stream`
values are spelled as strings (`"STREAM_STDOUT"`, `"STREAM_STDERR"`,
`"STREAM_BOTH"`).

Two kinds of edges:

- A **data edge** carries one node's stream (or a file) to a sink: another
  node's stdin, or a file.
- An **order edge** says one node must exit before another may start,
  without connecting their stdio at all.

### Example: a fan-out

`emit`'s stdout goes to **three** destinations at once — `count`'s stdin,
`evens`' stdin, and the file `all.txt` — and `report` waits for both
consumers to exit. All four nodes are spawned as soon as the graph permits:
`count` and `evens` start the moment `emit` does, not when it finishes.

```toml
[[nodes]]
name = "emit"
shell = true
command = ["seq 1 4"]

[[nodes]]
name = "count"
shell = true
command = ["wc -l"]

[[nodes]]
name = "evens"
shell = true
command = ["awk 'NR % 2 == 0'"]

[[nodes]]
name = "report"
shell = true
command = ["echo both consumers are done"]

# emit's stdout -> count's stdin
[[edges]]
[edges.data]
nodeStdin = "count"

[edges.data.node]
node = "emit"
stream = "STREAM_STDOUT"

# emit's stdout -> evens' stdin
[[edges]]
[edges.data]
nodeStdin = "evens"

[edges.data.node]
node = "emit"
stream = "STREAM_STDOUT"

# emit's stdout -> a file, truncated
[[edges]]
[edges.data]

[edges.data.fileSink]
path = "all.txt"

[edges.data.node]
node = "emit"
stream = "STREAM_STDOUT"

# report waits for both consumers to exit
[[edges]]
[edges.order]
after = "count"
node = "report"

[[edges]]
[edges.order]
after = "evens"
node = "report"
```

Neither consumer's stdout is claimed by an edge, so both pass through to
the terminal, tagged with the node they came from. The summary follows on
stderr:

```
$ orchestrator run fanout.toml
[count] 4
[evens] 2
[evens] 4
[report] both consumers are done
run summary:
  emit: succeeded
  count: succeeded
  evens: succeeded
  report: succeeded
$ cat all.txt
1
2
3
4
```

`count` and `evens` run concurrently, so the order their tagged lines
arrive in can differ between runs; each individual line is written whole,
never interleaved with another node's.

### The rest of the schema

Nodes:

| field | meaning |
| --- | --- |
| `name` | unique; referenced by edges and used in output tags |
| `command` | argv, or the whole command line as one string when `shell = true` |
| `shell` | run `sh -c "<command joined by spaces>"` instead of exec'ing argv |
| `env` | environment variables, merged on top of the inherited environment |
| `clearEnv` | start from an empty environment instead of inheriting |
| `workingDir` | directory to run in |
| `stdinFromParent` | inherit the orchestrator's own stdin fd (at most one node per graph; keeps TTY-ness, so a node that prompts still works) |

Data edges have one source and one sink; both halves are required.

- Source: `[edges.data.node]` with `node` and `stream`, or `file = "path"`
  to stream a file in. A missing `stream` means `STREAM_STDOUT`.
- Sink: `nodeStdin = "name"`, or `[edges.data.fileSink]` with `path` and
  optional `append = true` (file sinks truncate by default).
- A node has at most one inbound data edge; one source may have as many
  outbound ones as it likes.

Order edges take `after`, `node`, and optional `anyExit = true`, which
satisfies the edge however the upstream node exits — that is what makes a
cleanup/finalizer node expressible. Without it, an upstream failure skips
the downstream node.

`[options]`, all optional: `queueCapBytes` (per-sink queue bound in bytes,
default 1048576, must be at least 1), `noTags` (bool), `shutdownGraceMs`
(default 5000).

A graph using all of it:

```toml
[[nodes]]
name = "ask"
command = ["cat"]
stdinFromParent = true

[[nodes]]
name = "shout"
shell = true
command = ["tr a-z A-Z"]

[[nodes]]
name = "where"
command = ["pwd"]
workingDir = "/usr"

[[nodes]]
name = "sealed"
command = ["env"]
clearEnv = true
env = { GREETING = "hello" }

[[nodes]]
name = "cleanup"
shell = true
command = ["echo cleanup ran"]

# ask's stdout -> shout's stdin
[[edges]]
[edges.data]
nodeStdin = "shout"

[edges.data.node]
node = "ask"
stream = "STREAM_STDOUT"

# shout's stdout appended to a log rather than truncating it
[[edges]]
[edges.data]

[edges.data.fileSink]
path = "log.txt"
append = true

[edges.data.node]
node = "shout"
stream = "STREAM_STDOUT"

# cleanup runs however `sealed` exits
[[edges]]
[edges.order]
after = "sealed"
node = "cleanup"
anyExit = true

[options]
queueCapBytes = 65536
noTags = false
shutdownGraceMs = 2000
```

`ask` reads the orchestrator's stdin, `shout`'s output is appended to
`log.txt` (which already held a line), and nothing else is claimed by an
edge, so it passes through:

```
$ printf 'already here\n' > log.txt
$ echo hello world | orchestrator run everything.toml
[where] /usr
[sealed] GREETING=hello
[cleanup] cleanup ran
run summary:
  ask: succeeded
  shout: succeeded
  where: succeeded
  sealed: succeeded
  cleanup: succeeded
$ cat log.txt
already here
HELLO WORLD
```

## Validation

Validation runs before anything is spawned, reports *every* problem it
found rather than the first, and exits 2. It rejects cycles, duplicate node
names, edges naming unknown nodes, a half-specified data edge, more than
one inbound data edge on a node, more than one node reading the parent's
stdin, `STREAM_BOTH` alongside a `STREAM_STDOUT`/`STREAM_STDERR` edge from
the same node, unreadable input files, output paths whose directory is
missing or unwritable, two file sinks on one path, a path used as both sink
and source, and a `queueCapBytes` of 0.

It also rejects one shape that is **not** a cycle: a data edge `X -> Y`
where `Y` also waits, directly or transitively, for `X` to exit. Both edges
run `X -> Y`, so the dependency graph is acyclic, but `X` cannot exit until
`Y` drains the output it is throttled on and `Y` cannot start until `X` has
exited. It happens to work while the output fits in the queue, which is
worse than failing outright:

```
$ orchestrator validate deadlock.toml
node sink reads emit's output and also waits for emit to exit: emit cannot exit until sink has drained its output, and sink cannot start until emit has exited, so the run deadlocks as soon as the queue fills
$ echo $?
2
```

An order edge between two *siblings* of a fan-out is fine, and is the point
of order edges: a consumer that starts with its producer can exit while the
producer is still writing, so gating a third node on it deadlocks nothing.

Validation has **no filesystem side effects on output paths**: it never
creates, opens for write, or truncates a sink, and checks the parent
directory instead. Input files are opened for reading, which has no side
effects and is the only way to tell a readable file from one that merely
exists.

## Subcommands

```
orchestrator run <graph> [--format json|toml|pb] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]
orchestrator validate <graph> [--format ..] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]
orchestrator dot <graph> [--format ..] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]
```

- **`run`** executes the graph to completion.
- **`validate`** resolves and validates the graph and prints the resolved
  plan — every node with its stdin source and order dependencies, every
  fan-out with its destinations, and the effective options — but runs
  nothing. For `fanout.toml` above:

  ```
  $ orchestrator validate fanout.toml
  plan:
    options: queue_cap_bytes=1048576 tags=true shutdown_grace_ms=5000
    nodes:
      emit ["seq 1 4"]
      count ["wc -l"]
        stdin: emit's stdout
      evens ["awk 'NR % 2 == 0'"]
        stdin: emit's stdout
      report ["echo both consumers are done"]
        waits for: count, evens
    fanouts:
      emit's stdout -> count's stdin, evens's stdin, file all.txt (truncate)
      emit's stderr -> passthrough (stderr)
      count's stdout -> passthrough (stdout)
      count's stderr -> passthrough (stderr)
      evens's stdout -> passthrough (stdout)
      evens's stderr -> passthrough (stderr)
      report's stdout -> passthrough (stdout)
      report's stderr -> passthrough (stderr)
  ```

- **`dot`** prints the resolved plan as a Graphviz `digraph` on stdout, for
  piping into `dot -Tsvg`. Files are box-shaped; order edges are dashed:

  ```
  $ orchestrator dot fanout.toml
  digraph plan {
    n0 [label="emit"];
    n1 [label="count"];
    n2 [label="evens"];
    n3 [label="report"];
    n0 -> n1 [label="STREAM_STDOUT"];
    n0 -> n2 [label="STREAM_STDOUT"];
    f0 [shape=box label="all.txt"];
    n0 -> f0 [label="STREAM_STDOUT"];
    n1 -> n3 [style=dashed];
    n2 -> n3 [style=dashed];
  }
  ```

Flags, all optional, apply to every subcommand and override the graph
config's `[options]` table:

- `--format json|toml|pb` — overrides the format inferred from the file
  extension.
- `--no-tags` — disables the `[node] ` prefix on pass-through output.
- `--queue-cap-bytes N` — overrides the per-sink queue capacity.
- `--shutdown-grace-ms N` — overrides how long a child is given to exit on
  SIGTERM before the orchestrator sends SIGKILL.

## Exit codes

- **0** — every node succeeded, or stopped only because a consumer of its
  output went away: another node (`yes | head -1`) or the orchestrator's
  own stdout, when the orchestrator itself was piped into something that
  left. Both are reported, by name, rather than as a bare signal:

  ```
  $ orchestrator run big.toml | head -1
  [emit] 1
  run summary:
    emit: stopped when the orchestrator's stdout went away (killed by signal 13)
      note: stdout was closed from our end once the orchestrator's stdout went away
  $ echo "${PIPESTATUS[0]}"
  0
  ```

  (`$?` after a pipeline is `head`'s status, not the orchestrator's, hence
  `PIPESTATUS`.)

- **1** — any node failed, was skipped, had its input truncated or its
  output lost, or the run was interrupted by SIGINT/SIGTERM.
- **2** — the config failed to load or failed validation. This never
  depends on anything having run.

A script can tell "I wrote a bad graph" (2) from "the graph ran and
something in it broke" (1) from "it worked" (0).

## stdout/stderr contract

stdout carries only node pass-through output (`run`) or the digraph
(`dot`). Everything else — the run summary, config and validation errors —
goes to stderr. `validate` prints nothing to stdout.

## Development

The project's gate, all three of which must pass:

```
cargo test
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

`protoc` must be on `PATH` (or named by `$PROTOC`) for the build script.
