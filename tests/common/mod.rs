//! Shared harness for the end-to-end run tests: write a graph into a
//! temporary directory and invoke the real `orchestrator` binary on it.

use std::path::Path;
use std::process::{Child, Command, Output, Stdio};

/// Writes `toml` into `dir/graph.toml` and returns the path. Every
/// occurrence of `{dir}` in `toml` is replaced with `dir`, so fixtures can
/// name output files without knowing the temporary path.
fn write_graph(dir: &Path, toml: &str) -> std::path::PathBuf {
    let path = dir.join("graph.toml");
    let text = toml.replace("{dir}", &dir.display().to_string());
    std::fs::write(&path, text).expect("write graph.toml");
    path
}

fn command(dir: &Path, toml: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_orchestrator"));
    cmd.arg("run")
        .arg(write_graph(dir, toml))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Generous upper bound on any fixture in this suite. Every run is bounded
/// so that a scheduler that deadlocks fails the test instead of hanging the
/// whole suite with no diagnosis.
const RUN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Runs the graph to completion in `dir`, capturing stdout and stderr.
pub fn run_graph_in(dir: &Path, toml: &str) -> Output {
    wait_within(spawn_graph_in(dir, toml), RUN_TIMEOUT)
}

/// Runs the graph to completion in a temporary directory that is removed
/// afterwards — for graphs that write no files worth inspecting.
pub fn run_graph(toml: &str) -> Output {
    let dir = tempfile::tempdir().expect("create tempdir");
    run_graph_in(dir.path(), toml)
}

/// Starts the graph without waiting for it, so a test can observe files
/// appearing while nodes are still running.
pub fn spawn_graph_in(dir: &Path, toml: &str) -> Child {
    command(dir, toml)
        .spawn()
        .expect("spawn the orchestrator binary")
}

/// Runs the graph with the orchestrator's own stdout closed from under it,
/// as `orchestrator run g | head -1` does once `head` has left. The returned
/// `Output` has an empty `stdout` for that reason — the interesting part is
/// the status and the summary on stderr.
pub fn run_graph_with_broken_stdout(dir: &Path, toml: &str) -> Output {
    let mut child = spawn_graph_in(dir, toml);
    // Dropping our read end is what makes the orchestrator's writes to its
    // own stdout fail, exactly as a departed `head` would.
    drop(child.stdout.take().expect("stdout is piped"));
    wait_within(child, RUN_TIMEOUT)
}

/// Waits for a run that is supposed to finish, turning a hang into a test
/// failure instead of a hung suite.
///
/// The two output pipes are drained by their own threads throughout, so a
/// node that writes more than a pipe buffer's worth can never wedge the
/// harness itself — only the orchestrator is on trial here.
pub fn wait_within(mut child: Child, timeout: std::time::Duration) -> Output {
    use std::io::Read;

    fn drain(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<Vec<u8>> {
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = pipe.read_to_end(&mut bytes);
            bytes
        })
    }

    // `None` when the caller has deliberately closed the orchestrator's
    // stdout already — see `run_graph_with_broken_stdout`.
    let out = child.stdout.take().map(drain);
    let err = drain(child.stderr.take().expect("stderr is piped"));

    let start = std::time::Instant::now();
    let mut timed_out = false;
    let status = loop {
        match child.try_wait().expect("poll the orchestrator") {
            Some(status) => break status,
            None if start.elapsed() > timeout => {
                // Killing it closes the pipes, so the drain threads finish
                // and there is something to show in the panic message.
                let _ = child.kill();
                timed_out = true;
                break child.wait().expect("reap the orchestrator");
            }
            None => std::thread::sleep(std::time::Duration::from_millis(20)),
        }
    };

    let output = Output {
        status,
        stdout: out.map_or_else(Vec::new, |h| h.join().expect("drain stdout")),
        stderr: err.join().expect("drain stderr"),
    };
    assert!(
        !timed_out,
        "the run did not finish within {timeout:?} — it deadlocked\nstderr:\n{}",
        stderr(&output)
    );
    output
}

pub fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

pub fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The process exit code, with the summary included in the panic message
/// when the process died by a signal instead.
pub fn code(output: &Output) -> i32 {
    output.status.code().unwrap_or_else(|| {
        panic!(
            "the orchestrator did not exit normally: {:?}\nstderr:\n{}",
            output.status,
            stderr(output)
        )
    })
}

/// The summary line for `node`, with the `  <name>: ` prefix stripped.
/// Panics with the whole summary when the node is missing, which is the
/// failure mode worth seeing in full.
pub fn outcome_of(output: &Output, node: &str) -> String {
    let text = stderr(output);
    let prefix = format!("  {node}: ");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .map(str::to_string)
        .unwrap_or_else(|| panic!("no summary entry for {node} in:\n{text}"))
}

// --- graph fixture helpers -------------------------------------------------
//
// The TOML spelling is the one settled in `tests/config.rs`: pbjson
// camelCase field names and string enum values. Shell scripts are written
// in TOML literal strings (single quotes), so they must not contain a
// single quote themselves.

/// A node run via `sh -c`.
pub fn sh(name: &str, script: &str) -> String {
    assert!(
        !script.contains('\''),
        "a TOML literal string cannot contain a single quote: {script}"
    );
    format!("[[nodes]]\nname = \"{name}\"\nshell = true\ncommand = ['{script}']\n\n")
}

/// A node run directly, without a shell.
pub fn argv(name: &str, command: &[&str]) -> String {
    let args: Vec<String> = command.iter().map(|a| format!("\"{a}\"")).collect();
    format!(
        "[[nodes]]\nname = \"{name}\"\ncommand = [{}]\n\n",
        args.join(", ")
    )
}

/// A data edge from `from`'s stdout to `to`'s stdin.
pub fn data(from: &str, to: &str) -> String {
    format!(
        "[[edges]]\n[edges.data]\nnodeStdin = \"{to}\"\n\n[edges.data.node]\nnode = \"{from}\"\nstream = \"STREAM_STDOUT\"\n\n"
    )
}

/// A data edge from `from`'s stdout to a file.
pub fn data_to_file(from: &str, path: &str) -> String {
    format!(
        "[[edges]]\n[edges.data]\n\n[edges.data.fileSink]\npath = \"{path}\"\n\n[edges.data.node]\nnode = \"{from}\"\nstream = \"STREAM_STDOUT\"\n\n"
    )
}

/// A data edge from `from`'s **stderr** to `to`'s stdin.
pub fn data_stderr(from: &str, to: &str) -> String {
    format!(
        "[[edges]]\n[edges.data]\nnodeStdin = \"{to}\"\n\n[edges.data.node]\nnode = \"{from}\"\nstream = \"STREAM_STDERR\"\n\n"
    )
}

/// A data edge from `from`'s **stderr** to a file.
pub fn data_stderr_to_file(from: &str, path: &str) -> String {
    format!(
        "[[edges]]\n[edges.data]\n\n[edges.data.fileSink]\npath = \"{path}\"\n\n[edges.data.node]\nnode = \"{from}\"\nstream = \"STREAM_STDERR\"\n\n"
    )
}

/// A `[options]` table setting the per-sink queue cap. Must come last in a
/// fixture: every `[[nodes]]`/`[[edges]]` entry after it would be nested
/// inside it.
pub fn queue_cap_bytes(bytes: u64) -> String {
    format!("[options]\nqueueCapBytes = {bytes}\n")
}

/// An order edge: `node` waits for `after` to exit.
pub fn order(after: &str, node: &str) -> String {
    format!("[[edges]]\n[edges.order]\nafter = \"{after}\"\nnode = \"{node}\"\n\n")
}

/// An order edge that is satisfied however `after` exits.
pub fn order_any_exit(after: &str, node: &str) -> String {
    format!("[[edges]]\n[edges.order]\nafter = \"{after}\"\nnode = \"{node}\"\nanyExit = true\n\n")
}
