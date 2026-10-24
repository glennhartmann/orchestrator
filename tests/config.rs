use std::path::Path;

use orchestrator::config::{self, ConfigError, Format};
use orchestrator::pb;
use prost::Message;

// The brief's illustrative TOML repeats `[edges.data]`, which is not valid
// TOML. This is the settled, valid spelling: one `[edges.data]` table with
// the `nodeStdin` sink key, and a nested `[edges.data.node]` table for the
// `node` source (pbjson's camelCase field naming). This spelling is the
// contract — every later task's TOML fixtures should match it.
const TOML_GRAPH: &str = r#"
[[nodes]]
name = "a"
command = ["printf", "hi"]

[[nodes]]
name = "b"
command = ["cat"]

[[edges]]
[edges.data]
nodeStdin = "b"

[edges.data.node]
node = "a"
stream = "STREAM_STDOUT"
"#;

fn write_temp(name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "orchestrator-config-test-{}-{}",
        std::process::id(),
        name
    ));
    std::fs::write(&dir, bytes).expect("write temp config file");
    dir
}

#[test]
fn toml_json_and_binary_agree() {
    let toml_path = write_temp("graph.toml", TOML_GRAPH.as_bytes());
    let from_toml = config::load(&toml_path, None).expect("load toml");

    let json_bytes = serde_json::to_vec(&from_toml).expect("serialize graph to json");
    let json_path = write_temp("graph.json", &json_bytes);
    let from_json = config::load(&json_path, None).expect("load json");

    let mut pb_bytes = Vec::new();
    from_toml
        .encode(&mut pb_bytes)
        .expect("encode graph to binary");
    let pb_path = write_temp("graph.pb", &pb_bytes);
    let from_binary = config::load(&pb_path, None).expect("load binary");

    assert_eq!(from_toml, from_json);
    assert_eq!(from_toml, from_binary);
}

#[test]
fn format_comes_from_extension_and_flag_overrides_it() {
    assert_eq!(Format::from_path(Path::new("g.toml")), Some(Format::Toml));
    assert_eq!(Format::from_path(Path::new("g.json")), Some(Format::Json));
    assert_eq!(Format::from_path(Path::new("g.pb")), Some(Format::Binary));
    assert_eq!(Format::from_path(Path::new("g.txt")), None);

    let graph = pb::Graph {
        nodes: vec![pb::Node {
            name: "a".to_string(),
            command: vec!["true".to_string()],
            ..Default::default()
        }],
        edges: vec![],
        options: None,
    };
    let json_bytes = serde_json::to_vec(&graph).expect("serialize graph to json");
    // File named with the "wrong" extension; the explicit format flag must
    // override whatever Format::from_path would have guessed.
    let path = write_temp("graph.x", &json_bytes);

    let loaded = config::load(&path, Some(Format::Json)).expect("load with explicit format");
    assert_eq!(loaded, graph);
}

#[test]
fn malformed_config_is_an_error_not_a_panic() {
    let path = write_temp("malformed.toml", b"nodes = 3");

    let err = config::load(&path, None).expect_err("malformed config must error, not panic");
    assert!(matches!(err, ConfigError::Parse { .. }), "got: {err:?}");
}
