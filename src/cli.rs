//! Command-line surface: argument parsing only. `main` owns dispatch.

use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::Format;

/// Runs a DAG of programs with streamed stdout/stderr between them.
#[derive(Debug, Parser)]
#[command(name = "orchestrator", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the graph to completion.
    Run(GraphArgs),
    /// Resolve and validate the graph, printing the plan, but run nothing.
    Validate(GraphArgs),
    /// Print the resolved plan as a Graphviz `digraph` on stdout.
    Dot(GraphArgs),
}

/// Arguments shared by every subcommand: the graph file, its format, and
/// the `Options` overrides. These are applied to the config's `Options`
/// before `Plan::from_proto` runs, so they take effect identically for
/// `run`, `validate`, and `dot`.
#[derive(Debug, clap::Args)]
pub struct GraphArgs {
    /// Path to the graph config file.
    pub graph: PathBuf,

    /// The graph file's format. Inferred from the extension when omitted.
    #[arg(long, value_enum)]
    pub format: Option<CliFormat>,

    /// Disable `[tag]`-prefixed pass-through output.
    #[arg(long)]
    pub no_tags: bool,

    /// Override the per-sink queue capacity, in bytes.
    #[arg(long)]
    pub queue_cap_bytes: Option<u64>,

    /// Override the shutdown grace period, in milliseconds.
    #[arg(long)]
    pub shutdown_grace_ms: Option<u64>,
}

/// Mirrors [`Format`], as clap needs a type it can derive `ValueEnum` on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliFormat {
    Json,
    Toml,
    Pb,
}

impl From<CliFormat> for Format {
    fn from(f: CliFormat) -> Format {
        match f {
            CliFormat::Json => Format::Json,
            CliFormat::Toml => Format::Toml,
            CliFormat::Pb => Format::Binary,
        }
    }
}
