//! Resolving a validated `pb::Graph` into an execution `Plan`: name
//! resolution, every structural validation rule, and the DOT rendering
//! used by the CLI's `--dot` flag.
//!
//! Validation never touches the filesystem beyond `stat`-ing a path: no
//! output file is ever created, opened for write, or truncated here.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::pb;

const DEFAULT_QUEUE_CAP_BYTES: usize = 1024 * 1024;
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_millis(5000);

/// Index of a node within a [`Plan`]'s `nodes` vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub usize);

/// Where a node's stdin comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StdinSource {
    /// Nothing is written; stdin is closed.
    Empty,
    /// Inherited from the orchestrator's own stdin.
    Parent,
    /// Fed from another node's stream.
    Node { from: NodeId, stream: pb::Stream },
    /// Fed from a file on disk.
    File(PathBuf),
}

/// A `node` waits for `after` to exit before starting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderDep {
    pub after: NodeId,
    pub any_exit: bool,
}

/// One destination for a fanned-out stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkSpec {
    NodeStdin(NodeId),
    File {
        path: PathBuf,
        append: bool,
    },
    /// No data edge claimed this stream; it passes through to the
    /// orchestrator's own stdout/stderr.
    Passthrough(pb::Stream),
}

/// A resolved, validated node ready to be spawned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSpec {
    pub name: String,
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub clear_env: bool,
    pub working_dir: Option<PathBuf>,
    pub shell: bool,
    pub stdin: StdinSource,
    pub order_deps: Vec<OrderDep>,
}

/// One node's stream, broadcast to every sink that claimed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fanout {
    pub source: NodeId,
    pub stream: pb::Stream,
    pub sinks: Vec<SinkSpec>,
}

/// A fully resolved, validated execution plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub nodes: Vec<NodeSpec>,
    pub fanouts: Vec<Fanout>,
    pub queue_cap_bytes: usize,
    pub tags: bool,
    pub shutdown_grace: Duration,
}

/// Everything that can be wrong with a graph, discovered during
/// `Plan::from_proto`. Each variant carries the offending name(s)/path(s)
/// so an operator can find and fix the problem.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    #[error("duplicate node name: {0}")]
    DuplicateNodeName(String),
    #[error("edge refers to unknown node: {0}")]
    UnknownNode(String),
    #[error("dependency cycle: {}", .0.join(" -> "))]
    Cycle(Vec<String>),
    #[error("node {0} has more than one inbound data edge to its stdin")]
    MultipleStdinEdges(String),
    #[error("node {0} has a STREAM_BOTH data edge alongside a STDOUT/STDERR data edge")]
    ConflictingSourceStreams(String),
    #[error("more than one node reads stdin from the parent: {}", .0.join(", "))]
    MultipleParentStdin(Vec<String>),
    #[error("node {0} has an empty command")]
    EmptyCommand(String),
    #[error("file source does not exist or is not readable: {}", .0.display())]
    UnreadableFileSource(PathBuf),
    #[error(
        "output directory for {} is missing or not writable",
        .0.display()
    )]
    OutputDirUnusable(PathBuf),
    #[error("more than one data edge writes to the same file sink: {}", .0.display())]
    DuplicateFileSink(PathBuf),
    #[error("path {} is used as both a file sink and a file source", .0.display())]
    FileSinkUsedAsSource(PathBuf),
    #[error(
        "data edge from file {} straight to file sink {} is not supported; use `cp` instead",
        .source_path.display(), .sink_path.display()
    )]
    FileToFileEdge {
        source_path: PathBuf,
        sink_path: PathBuf,
    },
}

// --- resolution helpers, not part of the public API ------------------------

enum ResolvedSource {
    Node(NodeId, pb::Stream),
    File(PathBuf),
}

enum ResolvedSink {
    NodeStdin(NodeId),
    File(PathBuf, bool),
}

struct StdinEdge {
    target: NodeId,
    source: StdinSource,
}

fn resolve_node(name: &str, map: &HashMap<String, NodeId>) -> Option<NodeId> {
    map.get(name).copied()
}

/// Depth-first cycle detection over a directed adjacency list. Returns the
/// node names making up a cycle, if one exists.
fn find_cycle(adj: &[Vec<NodeId>], names: &[String]) -> Option<Vec<String>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    fn visit(
        u: usize,
        adj: &[Vec<NodeId>],
        color: &mut [Color],
        stack: &mut Vec<usize>,
    ) -> Option<Vec<usize>> {
        color[u] = Color::Gray;
        stack.push(u);
        for v in &adj[u] {
            match color[v.0] {
                Color::White => {
                    if let Some(cycle) = visit(v.0, adj, color, stack) {
                        return Some(cycle);
                    }
                }
                Color::Gray => {
                    let pos = stack
                        .iter()
                        .position(|&x| x == v.0)
                        .expect("gray node on stack");
                    let mut cycle: Vec<usize> = stack[pos..].to_vec();
                    cycle.push(v.0);
                    return Some(cycle);
                }
                Color::Black => {}
            }
        }
        stack.pop();
        color[u] = Color::Black;
        None
    }

    let mut color = vec![Color::White; adj.len()];
    let mut stack = Vec::new();
    for start in 0..adj.len() {
        if color[start] == Color::White
            && let Some(cycle) = visit(start, adj, &mut color, &mut stack)
        {
            return Some(cycle.into_iter().map(|i| names[i].clone()).collect());
        }
    }
    None
}

/// True if `dir` exists, is a directory, and appears writable.
fn dir_is_writable(dir: &Path) -> bool {
    match std::fs::metadata(dir) {
        Ok(meta) => meta.is_dir() && !meta.permissions().readonly(),
        Err(_) => false,
    }
}

fn sink_parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

impl Plan {
    pub fn node(&self, id: NodeId) -> &NodeSpec {
        &self.nodes[id.0]
    }

    /// Resolves and validates `g`, collecting *all* validation errors
    /// before returning so an operator fixing a graph sees the whole list.
    pub fn from_proto(g: pb::Graph) -> Result<Plan, Vec<ValidationError>> {
        let mut errors = Vec::new();

        // 1. Name resolution; duplicates keep the first mapping so later
        // references still resolve.
        let mut name_to_id: HashMap<String, NodeId> = HashMap::new();
        for (i, node) in g.nodes.iter().enumerate() {
            if name_to_id.insert(node.name.clone(), NodeId(i)).is_some() {
                errors.push(ValidationError::DuplicateNodeName(node.name.clone()));
            }
        }

        // 2. Per-node basics.
        let mut node_specs: Vec<NodeSpec> = g
            .nodes
            .iter()
            .map(|n| {
                if n.command.is_empty() {
                    errors.push(ValidationError::EmptyCommand(n.name.clone()));
                }
                NodeSpec {
                    name: n.name.clone(),
                    command: n.command.clone(),
                    env: n.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    clear_env: n.clear_env,
                    working_dir: n.working_dir.as_ref().map(PathBuf::from),
                    shell: n.shell,
                    stdin: StdinSource::Empty,
                    order_deps: Vec::new(),
                }
            })
            .collect();

        // 3. Parent stdin.
        let parent_stdin_names: Vec<String> = g
            .nodes
            .iter()
            .filter(|n| n.stdin_from_parent)
            .map(|n| n.name.clone())
            .collect();
        if parent_stdin_names.len() > 1 {
            errors.push(ValidationError::MultipleParentStdin(
                parent_stdin_names.clone(),
            ));
        }
        for name in &parent_stdin_names {
            if let Some(&id) = name_to_id.get(name.as_str()) {
                node_specs[id.0].stdin = StdinSource::Parent;
            }
        }

        // 4. Walk edges: resolve names, record order deps, and collect the
        // raw material for stdin assignment / fanout grouping / file checks.
        let mut dep_edges: Vec<(NodeId, NodeId)> = Vec::new(); // (after, dependent) for cycle detection
        let mut stdin_edges: Vec<StdinEdge> = Vec::new();
        let mut node_fanout_edges: Vec<(NodeId, pb::Stream, SinkSpec)> = Vec::new();
        let mut file_sink_paths: Vec<PathBuf> = Vec::new();
        let mut file_source_paths: HashSet<PathBuf> = HashSet::new();

        for edge in &g.edges {
            match &edge.kind {
                Some(pb::edge::Kind::Order(oe)) => {
                    let after = resolve_node(&oe.after, &name_to_id);
                    let node = resolve_node(&oe.node, &name_to_id);
                    if after.is_none() {
                        errors.push(ValidationError::UnknownNode(oe.after.clone()));
                    }
                    if node.is_none() {
                        errors.push(ValidationError::UnknownNode(oe.node.clone()));
                    }
                    if let (Some(a), Some(n)) = (after, node) {
                        dep_edges.push((a, n));
                        node_specs[n.0].order_deps.push(OrderDep {
                            after: a,
                            any_exit: oe.any_exit,
                        });
                    }
                }
                Some(pb::edge::Kind::Data(de)) => {
                    // Ruling E: a file-to-file edge is a `cp`, not a graph edge.
                    if let (
                        Some(pb::data_edge::Source::File(src_path)),
                        Some(pb::data_edge::Sink::FileSink(fs)),
                    ) = (&de.source, &de.sink)
                    {
                        errors.push(ValidationError::FileToFileEdge {
                            source_path: PathBuf::from(src_path),
                            sink_path: PathBuf::from(&fs.path),
                        });
                        continue;
                    }

                    let resolved_source = match &de.source {
                        Some(pb::data_edge::Source::Node(ns)) => {
                            match resolve_node(&ns.node, &name_to_id) {
                                Some(id) => {
                                    let stream = pb::Stream::try_from(ns.stream)
                                        .unwrap_or(pb::Stream::Unspecified);
                                    Some(ResolvedSource::Node(id, stream))
                                }
                                None => {
                                    errors.push(ValidationError::UnknownNode(ns.node.clone()));
                                    None
                                }
                            }
                        }
                        Some(pb::data_edge::Source::File(path)) => {
                            let p = PathBuf::from(path);
                            file_source_paths.insert(p.clone());
                            Some(ResolvedSource::File(p))
                        }
                        None => None,
                    };

                    let resolved_sink = match &de.sink {
                        Some(pb::data_edge::Sink::NodeStdin(name)) => {
                            match resolve_node(name, &name_to_id) {
                                Some(id) => Some(ResolvedSink::NodeStdin(id)),
                                None => {
                                    errors.push(ValidationError::UnknownNode(name.clone()));
                                    None
                                }
                            }
                        }
                        Some(pb::data_edge::Sink::FileSink(fs)) => {
                            let p = PathBuf::from(&fs.path);
                            file_sink_paths.push(p.clone());
                            Some(ResolvedSink::File(p, fs.append))
                        }
                        None => None,
                    };

                    match (resolved_source, resolved_sink) {
                        (
                            Some(ResolvedSource::Node(src_id, stream)),
                            Some(ResolvedSink::NodeStdin(sink_id)),
                        ) => {
                            dep_edges.push((src_id, sink_id));
                            stdin_edges.push(StdinEdge {
                                target: sink_id,
                                source: StdinSource::Node {
                                    from: src_id,
                                    stream,
                                },
                            });
                            node_fanout_edges.push((src_id, stream, SinkSpec::NodeStdin(sink_id)));
                        }
                        (
                            Some(ResolvedSource::Node(src_id, stream)),
                            Some(ResolvedSink::File(path, append)),
                        ) => {
                            node_fanout_edges.push((
                                src_id,
                                stream,
                                SinkSpec::File { path, append },
                            ));
                        }
                        (
                            Some(ResolvedSource::File(path)),
                            Some(ResolvedSink::NodeStdin(sink_id)),
                        ) => {
                            stdin_edges.push(StdinEdge {
                                target: sink_id,
                                source: StdinSource::File(path),
                            });
                        }
                        _ => {} // one or both sides already recorded their own error
                    }
                }
                None => {}
            }
        }

        // 5. Stdin assignment: at most one inbound data edge per node.
        let mut stdin_by_target: HashMap<NodeId, Vec<StdinSource>> = HashMap::new();
        for se in stdin_edges {
            stdin_by_target
                .entry(se.target)
                .or_default()
                .push(se.source);
        }
        for (target, mut sources) in stdin_by_target {
            if sources.len() > 1 {
                errors.push(ValidationError::MultipleStdinEdges(
                    node_specs[target.0].name.clone(),
                ));
            } else {
                node_specs[target.0].stdin = sources.pop().expect("non-empty by construction");
            }
        }

        // 6. Conflicting source streams: STREAM_BOTH may not coexist with
        // an explicit STDOUT or STDERR edge from the same source.
        let mut streams_by_source: HashMap<NodeId, HashSet<pb::Stream>> = HashMap::new();
        for (id, stream, _) in &node_fanout_edges {
            streams_by_source.entry(*id).or_default().insert(*stream);
        }
        for (id, streams) in &streams_by_source {
            if streams.contains(&pb::Stream::Both)
                && (streams.contains(&pb::Stream::Stdout) || streams.contains(&pb::Stream::Stderr))
            {
                errors.push(ValidationError::ConflictingSourceStreams(
                    node_specs[id.0].name.clone(),
                ));
            }
        }

        // 7. Group node-sourced data edges into fanouts, preserving the
        // order edges were declared in.
        let mut fanouts: Vec<Fanout> = Vec::new();
        let mut fanout_index: HashMap<(NodeId, pb::Stream), usize> = HashMap::new();
        for (id, stream, sink) in node_fanout_edges {
            let key = (id, stream);
            if let Some(&idx) = fanout_index.get(&key) {
                fanouts[idx].sinks.push(sink);
            } else {
                fanout_index.insert(key, fanouts.len());
                fanouts.push(Fanout {
                    source: id,
                    stream,
                    sinks: vec![sink],
                });
            }
        }

        // 8. Every stream not claimed by a data edge passes through.
        for i in 0..node_specs.len() {
            let id = NodeId(i);
            let covered_by_both = fanout_index.contains_key(&(id, pb::Stream::Both));
            for stream in [pb::Stream::Stdout, pb::Stream::Stderr] {
                if covered_by_both || fanout_index.contains_key(&(id, stream)) {
                    continue;
                }
                fanouts.push(Fanout {
                    source: id,
                    stream,
                    sinks: vec![SinkSpec::Passthrough(stream)],
                });
            }
        }

        // 9. Cycle detection over the union of data and order dependency
        // edges.
        let names: Vec<String> = node_specs.iter().map(|n| n.name.clone()).collect();
        let mut adj: Vec<Vec<NodeId>> = vec![Vec::new(); node_specs.len()];
        for (a, b) in &dep_edges {
            adj[a.0].push(*b);
        }
        if let Some(cycle) = find_cycle(&adj, &names) {
            errors.push(ValidationError::Cycle(cycle));
        }

        // 10. File checks — stat only, never open for write.
        for path in &file_source_paths {
            if std::fs::metadata(path).is_err() {
                errors.push(ValidationError::UnreadableFileSource(path.clone()));
            }
        }

        let mut sink_path_counts: HashMap<PathBuf, usize> = HashMap::new();
        for path in &file_sink_paths {
            *sink_path_counts.entry(path.clone()).or_insert(0) += 1;
        }
        for (path, count) in &sink_path_counts {
            if !dir_is_writable(&sink_parent(path)) {
                errors.push(ValidationError::OutputDirUnusable(path.clone()));
            }
            if *count > 1 {
                errors.push(ValidationError::DuplicateFileSink(path.clone()));
            }
            if file_source_paths.contains(path) {
                errors.push(ValidationError::FileSinkUsedAsSource(path.clone()));
            }
        }

        // 11. Options, with defaults.
        let queue_cap_bytes = g
            .options
            .as_ref()
            .and_then(|o| o.queue_cap_bytes)
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_QUEUE_CAP_BYTES);
        let tags = !g.options.as_ref().map(|o| o.no_tags).unwrap_or(false);
        let shutdown_grace = g
            .options
            .as_ref()
            .and_then(|o| o.shutdown_grace_ms)
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_SHUTDOWN_GRACE);

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(Plan {
            nodes: node_specs,
            fanouts,
            queue_cap_bytes,
            tags,
            shutdown_grace,
        })
    }

    /// Renders the plan as a Graphviz `digraph`: one node per [`NodeSpec`],
    /// solid stream-labelled edges for data flow, dashed edges for order
    /// dependencies, and box-shaped nodes for file sources/sinks.
    pub fn to_dot(&self) -> String {
        let mut out = String::from("digraph plan {\n");

        for (i, n) in self.nodes.iter().enumerate() {
            out += &format!("  n{i} [label={:?}];\n", n.name);
        }

        let mut file_ids: HashMap<PathBuf, String> = HashMap::new();
        let mut next_file_id = 0usize;
        let mut file_node = |out: &mut String, path: &Path| -> String {
            if let Some(id) = file_ids.get(path) {
                return id.clone();
            }
            let id = format!("f{next_file_id}");
            next_file_id += 1;
            *out += &format!(
                "  {id} [shape=box label={:?}];\n",
                path.display().to_string()
            );
            file_ids.insert(path.to_path_buf(), id.clone());
            id
        };

        for (i, n) in self.nodes.iter().enumerate() {
            if let StdinSource::File(path) = &n.stdin {
                let fid = file_node(&mut out, path);
                out += &format!("  {fid} -> n{i};\n");
            }
        }

        for f in &self.fanouts {
            let label = f.stream.as_str_name();
            for sink in &f.sinks {
                match sink {
                    SinkSpec::NodeStdin(id) => {
                        out += &format!("  n{} -> n{} [label={label:?}];\n", f.source.0, id.0);
                    }
                    SinkSpec::File { path, .. } => {
                        let fid = file_node(&mut out, path);
                        out += &format!("  n{} -> {fid} [label={label:?}];\n", f.source.0);
                    }
                    SinkSpec::Passthrough(_) => {}
                }
            }
        }

        for (i, n) in self.nodes.iter().enumerate() {
            for dep in &n.order_deps {
                out += &format!("  n{} -> n{i} [style=dashed];\n", dep.after.0);
            }
        }

        out += "}\n";
        out
    }
}
