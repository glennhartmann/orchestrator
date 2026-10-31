//! Placeholder entry point: enough of a CLI to run a graph file end to end
//! (`orchestrator [run] <graph>`), so the integration tests exercise the
//! real binary. Task 9 replaces this with the full clap CLI — flags,
//! `--format`, `--dot`, `--no-tags`, and proper usage text.

use std::path::Path;
use std::process::ExitCode;

use orchestrator::config;
use orchestrator::exec::run::run;
use orchestrator::graph::Plan;

/// Config and validation problems exit 2, so a script can tell "I wrote a
/// bad graph" from "the graph ran and something in it broke".
const EXIT_CONFIG_ERROR: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1).peekable();
    if args.peek().map(String::as_str) == Some("run") {
        args.next();
    }
    let Some(path) = args.next() else {
        eprintln!("usage: orchestrator run <graph>");
        return ExitCode::from(EXIT_CONFIG_ERROR);
    };

    let graph = match config::load(Path::new(&path), None) {
        Ok(graph) => graph,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(EXIT_CONFIG_ERROR);
        }
    };
    let plan = match Plan::from_proto(graph) {
        Ok(plan) => plan,
        Err(errors) => {
            for e in errors {
                eprintln!("{e}");
            }
            return ExitCode::from(EXIT_CONFIG_ERROR);
        }
    };

    let report = run(&plan).await;
    // The summary belongs on stderr: stdout carries only node output.
    let _ = report.write(&mut std::io::stderr());
    ExitCode::from(report.exit_code() as u8)
}
