// Serde route: pbjson's generated Serialize/Deserialize impls, used as-is
// for both JSON and TOML. Verified empirically (not just inferred) that
// pbjson's Deserialize impls round-trip through `toml::from_str` for this
// schema, including the `Stream` enum (string variant names) and the
// `DataEdge` oneofs (`source`/`sink`) — toml's Deserializer is
// self-describing enough to satisfy pbjson's `deserialize_any` /
// `deserialize_struct` calls. No fallback to `prost_build::type_attribute`
// + hand-derived `serde::Deserialize` was needed.
use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("orchestrator_descriptor.bin");

    let proto_file = "proto/orchestrator/v1/graph.proto";

    let mut config = prost_build::Config::new();
    config.file_descriptor_set_path(&descriptor_path);
    config
        .compile_protos(&[proto_file], &["proto"])
        .expect("failed to compile protos");

    let descriptor_set = std::fs::read(&descriptor_path).expect("failed to read descriptor set");

    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)
        .expect("failed to register descriptors")
        .build(&[".orchestrator.v1"])
        .expect("failed to build pbjson serde impls");

    println!("cargo:rerun-if-changed={proto_file}");
}
