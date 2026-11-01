# orchestrator

Runs a DAG of programs, streaming each node's stdout/stderr to the
destinations named in a graph config: another node's stdin, a file, or
(when nothing claims a stream) straight through to the orchestrator's own
stdout/stderr. Linux and macOS.

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

### Example: the diamond

`a`'s stdout feeds `c`'s stdin, and `c`'s stdout feeds `e`'s stdin. `b` and
`d` only gate `e` by order — they don't touch its stdin. Every node starts
as soon as its own dependencies are satisfied; nothing waits for a whole
"layer" to finish.

```toml
[[nodes]]
name = "a"
shell = true
command = ["printf hello"]

[[nodes]]
name = "b"
shell = true
command = ["true"]

[[nodes]]
name = "c"
command = ["cat"]

[[nodes]]
name = "d"
shell = true
command = ["true"]

[[nodes]]
name = "e"
command = ["cat"]

[[edges]]
[edges.data]
nodeStdin = "c"

[edges.data.node]
node = "a"
stream = "STREAM_STDOUT"

[[edges]]
[edges.data]
nodeStdin = "e"

[edges.data.node]
node = "c"
stream = "STREAM_STDOUT"

[[edges]]
[edges.order]
after = "a"
node = "b"

[[edges]]
[edges.order]
after = "a"
node = "d"

[[edges]]
[edges.order]
after = "b"
node = "e"

[[edges]]
[edges.order]
after = "d"
node = "e"
```

Running it (`orchestrator run diamond.toml`) prints `e`'s output — `a`'s
"hello", relayed through `c` to `e` — tagged with the node it came from,
followed by the run summary on stderr:

```
$ orchestrator run diamond.toml
[e] hello
run summary:
  a: succeeded
  b: succeeded
  c: succeeded
  d: succeeded
  e: succeeded
```

## Subcommands

```
orchestrator run <graph> [--format json|toml|pb] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]
orchestrator validate <graph> [--format ..] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]
orchestrator dot <graph> [--format ..] [--no-tags] [--queue-cap-bytes N] [--shutdown-grace-ms N]
```

- **`run`** executes the graph to completion.
- **`validate`** resolves and validates the graph and prints the resolved
  plan, but runs nothing. It never creates, opens for write, or truncates
  any output file — it is safe to point at a config whose outputs already
  exist. For `diamond.toml` above:

  ```
  $ orchestrator validate diamond.toml
  plan:
    options: queue_cap_bytes=1048576 tags=true shutdown_grace_ms=5000
    nodes:
      a ["printf hello"]
      b ["true"]
      c ["cat"]
      d ["true"]
      e ["cat"]
    fanouts:
      a's stdout -> c's stdin
      c's stdout -> e's stdin
      a's stderr -> passthrough (stderr)
      b's stdout -> passthrough (stdout)
      b's stderr -> passthrough (stderr)
      c's stderr -> passthrough (stderr)
      d's stdout -> passthrough (stdout)
      d's stderr -> passthrough (stderr)
      e's stdout -> passthrough (stdout)
      e's stderr -> passthrough (stderr)
  ```

- **`dot`** prints the resolved plan as a Graphviz `digraph` on stdout, for
  piping into `dot -Tsvg`:

  ```
  $ orchestrator dot diamond.toml
  digraph plan {
    n0 [label="a"];
    n1 [label="b"];
    n2 [label="c"];
    n3 [label="d"];
    n4 [label="e"];
    n0 -> n2 [label="STREAM_STDOUT"];
    n2 -> n4 [label="STREAM_STDOUT"];
    n0 -> n1 [style=dashed];
    n0 -> n3 [style=dashed];
    n1 -> n4 [style=dashed];
    n3 -> n4 [style=dashed];
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

- **0** — every node succeeded or was abandoned (its last consumer went
  away, e.g. `yes | head -1`).
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
