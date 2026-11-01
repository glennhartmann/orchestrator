//! Entry point: parse argv, load and validate the graph, apply any CLI
//! overrides to its `Options`, and dispatch to `run`, `validate`, or `dot`.
//!
//! Exit codes: 0 if every node succeeded or was abandoned, 1 if any node
//! failed/was skipped/lost output or the run was interrupted by a signal,
//! 2 for a config or validation error. The run summary and all diagnostics
//! go to stderr; stdout carries only node pass-through output (`run`) or
//! the `digraph` (`dot`).

mod cli;

use std::process::ExitCode;

use clap::Parser;

use cli::{Cli, Command, GraphArgs};
use orchestrator::config::{self, ConfigError};
use orchestrator::exec::run::run;
use orchestrator::graph::{Plan, ValidationError};
use orchestrator::pb;

/// Config and validation problems exit 2, so a script can tell "I wrote a
/// bad graph" from "the graph ran and something in it broke".
const EXIT_CONFIG_ERROR: u8 = 2;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    let args = match &cli.command {
        Command::Run(a) | Command::Validate(a) | Command::Dot(a) => a,
    };

    let plan = match load_plan(args) {
        Ok(plan) => plan,
        Err(code) => return ExitCode::from(code),
    };

    match cli.command {
        Command::Run(_) => {
            let report = run(&plan).await;
            // The summary belongs on stderr: stdout carries only the
            // nodes' own pass-through output.
            let _ = report.write(&mut std::io::stderr());
            ExitCode::from(report.exit_code() as u8)
        }
        Command::Validate(_) => {
            print_plan(&plan, &mut std::io::stderr());
            ExitCode::from(0)
        }
        Command::Dot(_) => {
            println!("{}", plan.to_dot());
            ExitCode::from(0)
        }
    }
}

/// Loads the config, applies the CLI's `Options` overrides, and resolves
/// it into a `Plan`. Any failure here is a config/validation problem, so
/// it reports on stderr and returns `EXIT_CONFIG_ERROR`.
fn load_plan(args: &GraphArgs) -> Result<Plan, u8> {
    let format = args.format.map(Into::into);
    let mut graph = config::load(&args.graph, format).map_err(|e: ConfigError| {
        eprintln!("{e}");
        EXIT_CONFIG_ERROR
    })?;

    apply_overrides(&mut graph, args);

    Plan::from_proto(graph).map_err(|errors: Vec<ValidationError>| {
        for e in errors {
            eprintln!("{e}");
        }
        EXIT_CONFIG_ERROR
    })
}

/// Applies the CLI's flag overrides onto the config's `Options`, in place,
/// before `Plan::from_proto` ever sees it. Flags that were not given leave
/// the config's value (and, if that is also unset, `Plan::from_proto`'s own
/// default) untouched.
fn apply_overrides(graph: &mut pb::Graph, args: &GraphArgs) {
    let options = graph.options.get_or_insert_with(Default::default);
    if let Some(bytes) = args.queue_cap_bytes {
        options.queue_cap_bytes = Some(bytes);
    }
    if args.no_tags {
        options.no_tags = true;
    }
    if let Some(ms) = args.shutdown_grace_ms {
        options.shutdown_grace_ms = Some(ms);
    }
}

/// Prints the resolved plan: every node, its fanouts, and the effective
/// options, so `validate` is useful for more than a bare pass/fail.
fn print_plan(plan: &Plan, w: &mut impl std::io::Write) {
    let _ = writeln!(w, "plan:");
    let _ = writeln!(
        w,
        "  options: queue_cap_bytes={} tags={} shutdown_grace_ms={}",
        plan.queue_cap_bytes,
        plan.tags,
        plan.shutdown_grace.as_millis()
    );
    let _ = writeln!(w, "  nodes:");
    for node in &plan.nodes {
        let _ = writeln!(w, "    {} {:?}", node.name, node.command);
    }
    let _ = writeln!(w, "  fanouts:");
    for fanout in &plan.fanouts {
        let _ = writeln!(
            w,
            "    {} ({:?}) -> {:?}",
            plan.nodes[fanout.source.0].name, fanout.stream, fanout.sinks
        );
    }
}
