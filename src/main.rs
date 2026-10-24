// The CLI (argument parsing, wiring config::load into a real command) lands
// in Task 9. For now this binary only needs to compile against the same
// modules the library exposes; nothing here uses them yet, hence the
// `allow(dead_code)`.
#![allow(dead_code)]

mod config;
mod pb;

fn main() {}
