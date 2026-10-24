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
        vec![node("a", &["true"]), node("b", &["cat"])],
        vec![
            data_node_to_stdin("a", pb::Stream::Stdout, "b"),
            order("a", "b", false),
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
