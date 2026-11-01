//! Behavioral tests for the `orchestrator` CLI: `run`, `validate`, and
//! `dot`, plus the flag overrides that must reach `Plan::from_proto` before
//! the plan is built.

mod common;

use std::process::{Command, Stdio};

use common::*;

fn orchestrator() -> Command {
    Command::new(env!("CARGO_BIN_EXE_orchestrator"))
}

fn write_graph(dir: &std::path::Path, name: &str, toml: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, toml).expect("write graph file");
    path
}

#[test]
fn validate_prints_the_resolved_plan_and_exits_zero() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("a", &["true"]) + &argv("b", &["true"]) + &data("a", "b");
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let printed = stderr(&out);
    assert!(
        printed.contains('a') && printed.contains('b'),
        "validate must print the resolved plan naming every node:\n{printed}"
    );
}

#[test]
fn validate_reports_every_error_at_once_and_exits_two() {
    let dir = tempfile::tempdir().unwrap();
    // "ghost" is unknown, and a -> b -> a is a cycle.
    let graph = argv("a", &["true"])
        + &argv("b", &["true"])
        + &order("a", "b")
        + &order("b", "a")
        + &order("a", "ghost");
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate");

    assert_eq!(code(&out), 2, "stderr:\n{}", stderr(&out));
    let printed = stderr(&out);
    assert!(
        printed.contains("ghost"),
        "the unknown-node error must be reported:\n{printed}"
    );
    assert!(
        printed.to_lowercase().contains("cycle"),
        "the cycle error must be reported:\n{printed}"
    );
}

#[test]
fn validate_does_not_create_or_truncate_output_files() {
    let dir = tempfile::tempdir().unwrap();
    let existing = dir.path().join("existing.out");
    std::fs::write(&existing, b"keep me").expect("seed existing output file");
    let missing = dir.path().join("missing.out");
    assert!(!missing.exists());

    let graph = argv("a", &["true"])
        + &data_to_file("a", &existing.display().to_string())
        + &argv("b", &["true"]);
    // b has no edge to `missing`; we just assert no stray creation happens.
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert_eq!(
        std::fs::read(&existing).expect("existing file must still be readable"),
        b"keep me",
        "validate must not truncate an existing output file"
    );
    assert!(
        !missing.exists(),
        "validate must not create files that were never there"
    );
}

#[test]
fn validate_rejects_a_missing_file_source_with_exit_two() {
    let dir = tempfile::tempdir().unwrap();
    let missing_source = dir.path().join("does-not-exist.txt");
    let graph = format!(
        "[[nodes]]\nname = \"a\"\ncommand = [\"cat\"]\n\n[[edges]]\n[edges.data]\nnodeStdin = \"a\"\nfile = \"{}\"\n",
        missing_source.display()
    );
    let path = write_graph(dir.path(), "graph.toml", &graph);
    assert!(!missing_source.exists());

    let out = orchestrator()
        .arg("validate")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate");

    assert_eq!(code(&out), 2, "stderr:\n{}", stderr(&out));
    assert!(
        !missing_source.exists(),
        "validate must not create the missing source file"
    );
}

#[test]
fn dot_emits_a_digraph_naming_every_node() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("a", &["true"]) + &argv("b", &["true"]) + &data("a", "b");
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let out = orchestrator()
        .arg("dot")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run dot");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let printed = stdout(&out);
    assert!(printed.contains("digraph"), "got:\n{printed}");
    assert!(
        printed.contains('a') && printed.contains('b'),
        "got:\n{printed}"
    );
}

#[test]
fn no_tags_flag_produces_raw_output() {
    let dir = tempfile::tempdir().unwrap();
    // Two concurrent producers writing to the parent's own stdout: with
    // tagging on, each line is prefixed with its node's name; with
    // --no-tags, lines pass through unprefixed.
    let graph = sh("a", "printf hello; echo");
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let tagged = orchestrator()
        .arg("run")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run tagged");
    assert_eq!(code(&tagged), 0, "stderr:\n{}", stderr(&tagged));
    assert!(
        stdout(&tagged).contains("a:") || stdout(&tagged).contains("[a]"),
        "tagging is on by default, output should be tagged: {:?}",
        stdout(&tagged)
    );

    let untagged = orchestrator()
        .arg("run")
        .arg("--no-tags")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run untagged");
    assert_eq!(code(&untagged), 0, "stderr:\n{}", stderr(&untagged));
    assert_eq!(
        stdout(&untagged),
        "hello\n",
        "--no-tags must produce raw, unprefixed output"
    );
}

#[test]
fn queue_cap_flag_overrides_the_config_value() {
    // The config asks for a 10 MiB queue cap; the CLI flag asks for 16
    // bytes. `validate` prints the plan's effective `queue_cap_bytes`, so
    // the printed value proves which one actually reached `Plan::from_proto`.
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("a", &["true"]) + &queue_cap_bytes(10 * 1024 * 1024);
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg("--queue-cap-bytes")
        .arg("16")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate with overridden queue cap");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let printed = stderr(&out);
    assert!(
        printed.contains("queue_cap_bytes=16"),
        "the --queue-cap-bytes flag must override the config's 10 MiB value:\n{printed}"
    );
    assert!(
        !printed.contains("10485760"),
        "the config's queue cap must not survive the override:\n{printed}"
    );
}

#[test]
fn shutdown_grace_flag_is_visible_in_validate_output() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("a", &["true"]);
    let path = write_graph(dir.path(), "graph.toml", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg("--shutdown-grace-ms")
        .arg("1234")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert!(
        stderr(&out).contains("1234"),
        "validate's printed plan must reflect the --shutdown-grace-ms override:\n{}",
        stderr(&out)
    );
}

#[test]
fn bad_config_path_exits_two() {
    let out = orchestrator()
        .arg("run")
        .arg("/definitely/does/not/exist/graph.toml")
        .stdin(Stdio::null())
        .output()
        .expect("run against missing config");

    assert_eq!(code(&out), 2, "stderr:\n{}", stderr(&out));
}

#[test]
fn format_flag_overrides_the_extension() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("a", &["true"]);
    // Named with a misleading extension; --format must override it.
    let path = write_graph(dir.path(), "graph.txt", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg("--format")
        .arg("toml")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate with explicit format");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
}

#[test]
fn unknown_format_without_extension_exits_two() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("a", &["true"]);
    let path = write_graph(dir.path(), "graph.txt", &graph);

    let out = orchestrator()
        .arg("validate")
        .arg(&path)
        .stdin(Stdio::null())
        .output()
        .expect("run validate without a resolvable format");

    assert_eq!(code(&out), 2, "stderr:\n{}", stderr(&out));
}
