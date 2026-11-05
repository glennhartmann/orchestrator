use std::path::PathBuf;
use std::time::Duration;

use orchestrator::graph::{NodeId, Plan, SinkSpec, StdinSource, ValidationError};
use orchestrator::pb;

// --- small builders to keep each test's graph readable -------------------

fn node(name: &str, command: &[&str]) -> pb::Node {
    pb::Node {
        name: name.to_string(),
        command: command.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

fn parent_stdin_node(name: &str, command: &[&str]) -> pb::Node {
    pb::Node {
        stdin_from_parent: true,
        ..node(name, command)
    }
}

fn data_node_to_stdin(src: &str, stream: pb::Stream, dst: &str) -> pb::Edge {
    pb::Edge {
        kind: Some(pb::edge::Kind::Data(pb::DataEdge {
            source: Some(pb::data_edge::Source::Node(pb::NodeStream {
                node: src.to_string(),
                stream: stream as i32,
            })),
            sink: Some(pb::data_edge::Sink::NodeStdin(dst.to_string())),
        })),
    }
}

fn data_node_to_file(src: &str, stream: pb::Stream, path: &str, append: bool) -> pb::Edge {
    pb::Edge {
        kind: Some(pb::edge::Kind::Data(pb::DataEdge {
            source: Some(pb::data_edge::Source::Node(pb::NodeStream {
                node: src.to_string(),
                stream: stream as i32,
            })),
            sink: Some(pb::data_edge::Sink::FileSink(pb::FileSink {
                path: path.to_string(),
                append,
            })),
        })),
    }
}

fn data_file_to_stdin(path: &str, dst: &str) -> pb::Edge {
    pb::Edge {
        kind: Some(pb::edge::Kind::Data(pb::DataEdge {
            source: Some(pb::data_edge::Source::File(path.to_string())),
            sink: Some(pb::data_edge::Sink::NodeStdin(dst.to_string())),
        })),
    }
}

fn data_file_to_file(src_path: &str, sink_path: &str, append: bool) -> pb::Edge {
    pb::Edge {
        kind: Some(pb::edge::Kind::Data(pb::DataEdge {
            source: Some(pb::data_edge::Source::File(src_path.to_string())),
            sink: Some(pb::data_edge::Sink::FileSink(pb::FileSink {
                path: sink_path.to_string(),
                append,
            })),
        })),
    }
}

/// A data edge with whichever halves the caller supplies — including
/// neither, which is what an `[[edges]]` entry with an empty `[edges.data]`
/// table deserializes to.
fn data_edge(source: Option<pb::data_edge::Source>, sink: Option<pb::data_edge::Sink>) -> pb::Edge {
    pb::Edge {
        kind: Some(pb::edge::Kind::Data(pb::DataEdge { source, sink })),
    }
}

fn node_source(name: &str) -> pb::data_edge::Source {
    pb::data_edge::Source::Node(pb::NodeStream {
        node: name.to_string(),
        stream: pb::Stream::Stdout as i32,
    })
}

fn order(after: &str, downstream: &str, any_exit: bool) -> pb::Edge {
    pb::Edge {
        kind: Some(pb::edge::Kind::Order(pb::OrderEdge {
            after: after.to_string(),
            node: downstream.to_string(),
            any_exit,
        })),
    }
}

fn graph(nodes: Vec<pb::Node>, edges: Vec<pb::Edge>) -> pb::Graph {
    pb::Graph {
        nodes,
        edges,
        options: None,
    }
}

fn tempdir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "orchestrator-validate-test-{}-{}",
        std::process::id(),
        name
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

// --- rule tests ------------------------------------------------------------

#[test]
fn rejects_cycles() {
    // a's stdout feeds b's stdin (data edge a -> b), and an order edge
    // makes a wait on b (order edge b -> a). Together: a -> b -> a.
    let g = graph(
        vec![node("a", &["true"]), node("b", &["true"])],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "b"),
            order("b", "a", false),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("cycle must be rejected");
    assert!(
        errs.iter().any(|e| matches!(e, ValidationError::Cycle(_))),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_two_inbound_data_edges() {
    let g = graph(
        vec![
            node("a", &["true"]),
            node("b", &["true"]),
            node("c", &["cat"]),
        ],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "c"),
            data_node_to_stdin("b", pb::Stream::Stdout, "c"),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("two inbound data edges must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::MultipleStdinEdges(n) if n == "c")),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_duplicate_node_names() {
    let g = graph(vec![node("a", &["true"]), node("a", &["false"])], vec![]);
    let errs = Plan::from_proto(g).expect_err("duplicate names must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::DuplicateNodeName(n) if n == "a")),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_edge_to_unknown_node() {
    let g = graph(vec![node("a", &["true"])], vec![order("a", "ghost", false)]);
    let errs = Plan::from_proto(g).expect_err("unknown node must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::UnknownNode(n) if n == "ghost")),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_both_alongside_stdout_source() {
    let g = graph(
        vec![
            node("a", &["true"]),
            node("b", &["cat"]),
            node("c", &["cat"]),
        ],
        vec![
            data_node_to_stdin("a", pb::Stream::Both, "b"),
            data_node_to_stdin("a", pb::Stream::Stdout, "c"),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("conflicting stream sources must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::ConflictingSourceStreams(n) if n == "a")),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_two_nodes_reading_parent_stdin() {
    let g = graph(
        vec![
            parent_stdin_node("a", &["cat"]),
            parent_stdin_node("b", &["cat"]),
        ],
        vec![],
    );
    let errs = Plan::from_proto(g).expect_err("two parent-stdin readers must be rejected");
    assert!(
        errs.iter().any(|e| matches!(
            e,
            ValidationError::MultipleParentStdin(names)
                if names.len() == 2 && names.contains(&"a".to_string()) && names.contains(&"b".to_string())
        )),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_parent_stdin_with_inbound_data_edge() {
    // `b` reads stdin from the parent AND has an inbound data edge from
    // `a`'s stdout: two sources for one stdin, which is exactly the
    // ambiguity "at most one inbound data edge per node" forbids.
    let g = graph(
        vec![node("a", &["true"]), parent_stdin_node("b", &["cat"])],
        vec![data_node_to_stdin("a", pb::Stream::Stdout, "b")],
    );
    let errs = Plan::from_proto(g).expect_err("conflicting stdin sources must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::ConflictingStdinSources(n) if n == "b")),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_empty_command() {
    let g = graph(vec![node("a", &[])], vec![]);
    let errs = Plan::from_proto(g).expect_err("empty command must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::EmptyCommand(n) if n == "a")),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_unreadable_file_source() {
    let g = graph(
        vec![node("a", &["cat"])],
        vec![data_file_to_stdin(
            "/definitely/does/not/exist/orchestrator-test-input.txt",
            "a",
        )],
    );
    let errs = Plan::from_proto(g).expect_err("unreadable file source must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::UnreadableFileSource(_))),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_output_dir_missing_or_unwritable() {
    // Missing parent directory.
    let g = graph(
        vec![node("a", &["true"])],
        vec![data_node_to_file(
            "a",
            pb::Stream::Stdout,
            "/definitely/does/not/exist/out.txt",
            false,
        )],
    );
    let errs = Plan::from_proto(g).expect_err("missing output dir must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::OutputDirUnusable(_))),
        "got: {errs:?}"
    );

    // Parent directory exists but is not writable.
    let dir = tempdir("readonly-dir");
    let mut perms = std::fs::metadata(&dir).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o555);
    std::fs::set_permissions(&dir, perms).expect("make dir readonly");

    let sink_path = dir.join("out.txt");
    let g = graph(
        vec![node("a", &["true"])],
        vec![data_node_to_file(
            "a",
            pb::Stream::Stdout,
            sink_path.to_str().unwrap(),
            false,
        )],
    );
    let errs = Plan::from_proto(g).expect_err("unwritable output dir must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::OutputDirUnusable(_))),
        "got: {errs:?}"
    );

    // Restore permissions so the temp dir can be cleaned up by the OS.
    let mut perms = std::fs::metadata(&dir).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
    std::fs::set_permissions(&dir, perms).ok();
}

#[test]
fn rejects_two_file_sinks_on_one_path() {
    let dir = tempdir("dup-sink");
    let sink_path = dir.join("out.txt");
    let g = graph(
        vec![node("a", &["true"]), node("b", &["true"])],
        vec![
            data_node_to_file("a", pb::Stream::Stdout, sink_path.to_str().unwrap(), false),
            data_node_to_file("b", pb::Stream::Stdout, sink_path.to_str().unwrap(), false),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("two sinks on one path must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::DuplicateFileSink(p) if *p == sink_path)),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_sink_path_also_used_as_source() {
    let dir = tempdir("sink-as-source");
    let shared_path = dir.join("shared.txt");
    std::fs::write(&shared_path, b"existing").expect("seed shared file");

    let g = graph(
        vec![node("a", &["true"]), node("b", &["cat"])],
        vec![
            data_node_to_file(
                "a",
                pb::Stream::Stdout,
                shared_path.to_str().unwrap(),
                false,
            ),
            data_file_to_stdin(shared_path.to_str().unwrap(), "b"),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("reused path must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::FileSinkUsedAsSource(p) if *p == shared_path)),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_file_to_file_edge() {
    let dir = tempdir("file-to-file");
    let src_path = dir.join("in.txt");
    std::fs::write(&src_path, b"data").expect("seed source file");
    let sink_path = dir.join("out.txt");

    let g = graph(
        vec![],
        vec![data_file_to_file(
            src_path.to_str().unwrap(),
            sink_path.to_str().unwrap(),
            false,
        )],
    );
    let errs = Plan::from_proto(g).expect_err("file-to-file edge must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::FileToFileEdge { .. })),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_a_data_edge_with_no_source() {
    // Measured before this rule existed: the edge was silently dropped, `b`
    // ran on an empty stdin, and the run reported success.
    let g = graph(
        vec![node("a", &["true"]), node("b", &["cat"])],
        vec![data_edge(
            None,
            Some(pb::data_edge::Sink::NodeStdin("b".to_string())),
        )],
    );
    let errs = Plan::from_proto(g).expect_err("a sourceless data edge must be rejected");
    assert!(
        errs.iter().any(|e| matches!(
            e,
            ValidationError::IncompleteDataEdge { index: 0, missing } if missing == "source"
        )),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_a_data_edge_with_no_sink() {
    let g = graph(
        vec![node("a", &["true"])],
        vec![data_edge(Some(node_source("a")), None)],
    );
    let errs = Plan::from_proto(g).expect_err("a sinkless data edge must be rejected");
    assert!(
        errs.iter().any(|e| matches!(
            e,
            ValidationError::IncompleteDataEdge { index: 0, missing } if missing == "sink"
        )),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_a_data_edge_with_neither_half() {
    let g = graph(vec![node("a", &["true"])], vec![data_edge(None, None)]);
    let errs = Plan::from_proto(g).expect_err("an empty data edge must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::IncompleteDataEdge { index: 0, .. })),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_an_edge_with_no_kind() {
    let g = graph(vec![node("a", &["true"])], vec![pb::Edge { kind: None }]);
    let errs = Plan::from_proto(g).expect_err("an edge that is neither data nor order is a typo");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::EdgeWithoutKind(0))),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_an_order_edge_from_a_node_to_its_own_data_consumer() {
    // `b` reads `a`'s stdout and also waits for `a` to exit. Not a cycle in
    // the dependency union — both edges run a -> b — but a guaranteed
    // deadlock once a's output fills b's queue: a cannot exit until b
    // drains, and b cannot start until a exits.
    let g = graph(
        vec![node("a", &["true"]), node("b", &["cat"])],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "b"),
            order("a", "b", false),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("the deadlock shape must be rejected");
    assert!(
        errs.iter().any(|e| matches!(
            e,
            ValidationError::OrderDependencyOnDataProducer { producer, consumer }
                if producer == "a" && consumer == "b"
        )),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_a_transitive_order_path_back_to_a_data_producer() {
    // a -> c as data; c waits for b, and b waits for a. The order path
    // c <- b <- a means c still cannot start until a has exited, so it
    // deadlocks on exactly the same grounds as the direct case.
    let g = graph(
        vec![
            node("a", &["true"]),
            node("b", &["true"]),
            node("c", &["cat"]),
        ],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "c"),
            order("a", "b", false),
            order("b", "c", false),
        ],
    );
    let errs = Plan::from_proto(g).expect_err("a transitive order path must be rejected too");
    assert!(
        errs.iter().any(|e| matches!(
            e,
            ValidationError::OrderDependencyOnDataProducer { producer, consumer }
                if producer == "a" && consumer == "c"
        )),
        "got: {errs:?}"
    );
}

#[test]
fn accepts_an_order_edge_from_a_sibling_consumer() {
    // The spec's own example, which the deadlock rule must not touch: `a`
    // fans out to `b` and `c`, and `c` additionally waits for `b`. `b`
    // starts as soon as `a` starts, so `b` can exit while `a` is still
    // producing, and `c` then runs. No order path leads from `a` to `c`.
    let g = graph(
        vec![
            node("a", &["true"]),
            node("b", &["cat"]),
            node("c", &["cat"]),
        ],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "b"),
            data_node_to_stdin("a", pb::Stream::Stdout, "c"),
            order("b", "c", false),
        ],
    );
    Plan::from_proto(g).expect("an order edge between siblings of a fan-out is legal");
}

#[test]
fn rejects_a_zero_queue_cap() {
    // Clamped to a 1-byte read loop internally, which turned 1.3 MB into
    // 34 seconds. It is a config mistake, so it is rejected up front.
    let mut g = graph(vec![node("a", &["true"])], vec![]);
    g.options = Some(pb::Options {
        queue_cap_bytes: Some(0),
        ..Default::default()
    });
    let errs = Plan::from_proto(g).expect_err("a zero queue cap must be rejected");
    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::ZeroQueueCap)),
        "got: {errs:?}"
    );
}

#[test]
fn rejects_a_file_source_that_exists_but_cannot_be_read() {
    // `fs::metadata` succeeds on a mode-000 file, so a stat-only check let
    // this through validation and failed at spawn time instead.
    let dir = tempdir("unreadable-source");
    let path = dir.join("secret.txt");
    std::fs::write(&path, b"data").expect("seed source file");
    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o000);
    std::fs::set_permissions(&path, perms).expect("make file unreadable");

    let g = graph(
        vec![node("a", &["cat"])],
        vec![data_file_to_stdin(path.to_str().unwrap(), "a")],
    );
    let errs = Plan::from_proto(g).expect_err("an unreadable file source must be rejected");

    let mut perms = std::fs::metadata(&path).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o644);
    std::fs::set_permissions(&path, perms).ok();

    assert!(
        errs.iter()
            .any(|e| matches!(e, ValidationError::UnreadableFileSource(p) if *p == path)),
        "got: {errs:?}"
    );
}

#[test]
fn a_fifo_file_source_does_not_block_validation() {
    // `File::open` on a FIFO with no writer blocks until one arrives, where
    // the old `fs::metadata` check returned immediately. `validate` is a
    // cheap pre-flight, so the readability probe must never wait on another
    // process. Run on its own thread so a regression fails the test instead
    // of hanging the suite forever.
    let dir = tempdir("fifo-source");
    let path = dir.join("pipe");
    let _ = std::fs::remove_file(&path);
    nix::unistd::mkfifo(&path, nix::sys::stat::Mode::from_bits_truncate(0o644))
        .expect("create a FIFO");

    let g = graph(
        vec![node("a", &["cat"])],
        vec![data_file_to_stdin(path.to_str().unwrap(), "a")],
    );
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(Plan::from_proto(g).is_ok());
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(accepted) => assert!(accepted, "a readable FIFO is a usable file source"),
        Err(_) => panic!("validation blocked on a FIFO with no writer"),
    }
}

#[test]
fn validation_creates_no_files() {
    let dir = tempdir("no-side-effects");
    let sink_path = dir.join("out.txt");
    assert!(!sink_path.exists());

    let g = graph(
        vec![node("a", &["true"])],
        vec![data_node_to_file(
            "a",
            pb::Stream::Stdout,
            sink_path.to_str().unwrap(),
            false,
        )],
    );
    let plan = Plan::from_proto(g).expect("valid graph");
    assert_eq!(plan.nodes.len(), 1);
    assert!(
        !sink_path.exists(),
        "from_proto must not create the sink file"
    );
}

#[test]
fn unredirected_streams_become_passthrough_sinks() {
    let g = graph(
        vec![node("a", &["true"]), node("b", &["cat"])],
        vec![data_node_to_stdin("a", pb::Stream::Stdout, "b")],
    );
    let plan = Plan::from_proto(g).expect("valid graph");
    let a = NodeId(0);
    let b = NodeId(1);

    // a's stdout is redirected: no passthrough sink for it, only the
    // explicit sink into b's stdin.
    let a_stdout = plan
        .fanouts
        .iter()
        .find(|f| f.source == a && f.stream == pb::Stream::Stdout)
        .expect("a's stdout fanout");
    assert_eq!(a_stdout.sinks, vec![SinkSpec::NodeStdin(b)]);

    // a's stderr is NOT redirected: it gets a passthrough sink.
    let a_stderr = plan
        .fanouts
        .iter()
        .find(|f| f.source == a && f.stream == pb::Stream::Stderr)
        .expect("a's stderr fanout");
    assert_eq!(
        a_stderr.sinks,
        vec![SinkSpec::Passthrough(pb::Stream::Stderr)]
    );

    // b has no outgoing data edges at all: both streams are passthrough.
    let b_stdout = plan
        .fanouts
        .iter()
        .find(|f| f.source == b && f.stream == pb::Stream::Stdout)
        .expect("b's stdout fanout");
    assert_eq!(
        b_stdout.sinks,
        vec![SinkSpec::Passthrough(pb::Stream::Stdout)]
    );
}

#[test]
fn stream_both_covers_both_streams() {
    let g = graph(
        vec![node("a", &["true"]), node("b", &["cat"])],
        vec![data_node_to_stdin("a", pb::Stream::Both, "b")],
    );
    let plan = Plan::from_proto(g).expect("valid graph");
    let a = NodeId(0);

    let a_fanouts: Vec<_> = plan.fanouts.iter().filter(|f| f.source == a).collect();
    assert_eq!(a_fanouts.len(), 1, "got: {a_fanouts:?}");
    assert_eq!(a_fanouts[0].stream, pb::Stream::Both);

    assert!(
        !plan
            .fanouts
            .iter()
            .any(|f| f.source == a && f.stream == pb::Stream::Stdout),
        "a must not have a separate stdout passthrough fanout"
    );
    assert!(
        !plan
            .fanouts
            .iter()
            .any(|f| f.source == a && f.stream == pb::Stream::Stderr),
        "a must not have a separate stderr passthrough fanout"
    );
}

#[test]
fn defaults_applied() {
    let g = graph(vec![node("a", &["true"])], vec![]);
    let plan = Plan::from_proto(g).expect("valid graph");
    assert_eq!(plan.queue_cap_bytes, 1024 * 1024);
    assert!(plan.tags);
    assert_eq!(plan.shutdown_grace, Duration::from_millis(5000));
}

#[test]
fn diamond_resolves() {
    // A -> {B, C, D} -> E, but only C's stdout actually feeds E's stdin;
    // B and D are pure order dependencies.
    let g = graph(
        vec![
            node("a", &["true"]),
            node("b", &["true"]),
            node("c", &["true"]),
            node("d", &["true"]),
            node("e", &["cat"]),
        ],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "b"),
            data_node_to_stdin("a", pb::Stream::Stdout, "c"),
            data_node_to_stdin("a", pb::Stream::Stdout, "d"),
            data_node_to_stdin("c", pb::Stream::Stdout, "e"),
            order("b", "e", false),
            order("d", "e", false),
        ],
    );
    let plan = Plan::from_proto(g).expect("valid graph");
    let (a, b, c, d, e) = (NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4));

    assert_eq!(
        plan.node(e).stdin,
        StdinSource::Node {
            from: c,
            stream: pb::Stream::Stdout
        }
    );
    assert_eq!(
        plan.node(e)
            .order_deps
            .iter()
            .map(|d| d.after)
            .collect::<Vec<_>>(),
        vec![b, d]
    );

    let a_stdout = plan
        .fanouts
        .iter()
        .find(|f| f.source == a && f.stream == pb::Stream::Stdout)
        .expect("a's stdout fanout");
    assert_eq!(
        a_stdout.sinks,
        vec![
            SinkSpec::NodeStdin(b),
            SinkSpec::NodeStdin(c),
            SinkSpec::NodeStdin(d),
        ]
    );
}

#[test]
fn to_dot_renders_nodes_and_edges() {
    let g = graph(
        vec![
            node("a", &["true"]),
            node("b", &["cat"]),
            node("c", &["true"]),
        ],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "b"),
            order("b", "c", false),
        ],
    );
    let plan = Plan::from_proto(g).expect("valid graph");
    let dot = plan.to_dot();

    assert!(dot.contains("digraph"), "got:\n{dot}");
    assert!(dot.contains('a'), "got:\n{dot}");
    assert!(dot.contains('b'), "got:\n{dot}");
    assert!(
        dot.contains("STREAM_STDOUT"),
        "data edge must carry its stream label:\n{dot}"
    );
    assert!(
        dot.contains("dashed"),
        "order edge must be rendered dashed:\n{dot}"
    );
}
