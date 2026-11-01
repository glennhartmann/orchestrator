//! Signal handling: SIGTERM/SIGINT forwarding to children, the grace
//! period, the SIGKILL fallback, and no-orphans on every exit path.

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

use common::*;

/// A `[options]` table setting the shutdown grace period. Must come last in
/// a fixture, like `queue_cap_bytes`.
fn shutdown_grace_ms(ms: u64) -> String {
    format!("[options]\nshutdownGraceMs = {ms}\n")
}

/// Polls `path` until it exists and has non-empty contents, returning the
/// parsed pid. Panics if it never appears within `timeout` — the fixture is
/// broken, not slow.
fn wait_for_pid(path: &Path, timeout: Duration) -> i32 {
    let start = Instant::now();
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return trimmed
                    .parse()
                    .unwrap_or_else(|e| panic!("pid file {path:?} contained {trimmed:?}: {e}"));
            }
        }
        if start.elapsed() > timeout {
            panic!("{path:?} never received a pid within {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Whether a pid is still alive, checked with a real signal-0 probe rather
/// than trusting that the orchestrator "said" it was gone.
fn alive(pid: i32) -> bool {
    signal::kill(Pid::from_raw(pid), None).is_ok()
}

fn assert_gone(pid: i32, who: &str) {
    // Signal delivery and process teardown are not instantaneous even after
    // the orchestrator itself has exited, so give it a brief window before
    // declaring an orphan.
    let start = Instant::now();
    while alive(pid) && start.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!alive(pid), "{who} (pid {pid}) is still alive — orphaned");
}

#[test]
fn sigterm_stops_scheduling_and_terminates_children() {
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "echo $$ > {dir}/a.pid; sleep 60")
        + &sh("b", "echo $$ > {dir}/b.pid; sleep 60")
        + &shutdown_grace_ms(2000);

    let child = spawn_graph_in(dir.path(), &graph);
    let orchestrator_pid = child.id() as i32;

    let a_pid = wait_for_pid(&dir.path().join("a.pid"), Duration::from_secs(10));
    let b_pid = wait_for_pid(&dir.path().join("b.pid"), Duration::from_secs(10));
    assert!(alive(a_pid), "a should be running before the signal");
    assert!(alive(b_pid), "b should be running before the signal");

    let start = Instant::now();
    signal::kill(Pid::from_raw(orchestrator_pid), Signal::SIGTERM)
        .expect("send SIGTERM to the orchestrator");

    let out = wait_within(child, Duration::from_secs(10));
    let elapsed = start.elapsed();

    assert_ne!(
        code(&out),
        0,
        "a signalled run must exit nonzero\nstderr:\n{}",
        stderr(&out)
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "shutdown took {elapsed:?} against a 2s grace with children that die the instant \
         they're SIGTERMed — it must have unconditionally slept out the grace instead of \
         returning as soon as both children were gone"
    );

    assert_gone(a_pid, "a's sleep");
    assert_gone(b_pid, "b's sleep");
}

#[test]
fn a_child_that_ignores_sigterm_is_killed_after_the_grace_period() {
    let dir = tempfile::tempdir().unwrap();
    let graph = sh(
        "stubborn",
        "trap \"\" TERM; echo $$ > {dir}/stubborn.pid; sleep 60",
    ) + &shutdown_grace_ms(500);

    let child = spawn_graph_in(dir.path(), &graph);
    let orchestrator_pid = child.id() as i32;

    let pid = wait_for_pid(&dir.path().join("stubborn.pid"), Duration::from_secs(10));
    assert!(alive(pid), "the stubborn child should be running");

    let start = Instant::now();
    signal::kill(Pid::from_raw(orchestrator_pid), Signal::SIGTERM)
        .expect("send SIGTERM to the orchestrator");

    let out = wait_within(child, Duration::from_secs(10));
    let elapsed = start.elapsed();

    assert_ne!(
        code(&out),
        0,
        "a signalled run must exit nonzero\nstderr:\n{}",
        stderr(&out)
    );
    assert!(
        elapsed >= Duration::from_millis(400),
        "shutdown finished in {elapsed:?}, before the 500ms grace even elapsed — \
         the ignored SIGTERM must have been mistaken for a clean exit"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "shutdown took {elapsed:?} — a child that ignores SIGTERM must still be \
         SIGKILLed once the grace period is up, not hang the orchestrator"
    );

    assert_gone(pid, "the stubborn child");
}

#[test]
fn a_node_still_gated_behind_an_order_edge_when_the_signal_lands_is_reported_sensibly() {
    // `gated`'s order edge is `anyExit = true`, so it tolerates `a` failing
    // — `a` dying from the forwarded SIGTERM does not block or
    // skip-propagate to `gated` (see `propagate_failure`'s `blocked`
    // check). That leaves `gated` genuinely never started and never
    // decided by anything else: it can only be decided by `finish`'s
    // stranded-node fallback, deterministically, because scheduling was
    // stopped (`self.interrupted`) before `a`'s exit was even processed.
    // That fallback must not render through the `Skipped { because }`
    // template, which is meant for a named culprit node and reads as
    // broken English when fed a shutdown reason instead ("skipped (the
    // orchestrator was shut down by a signal failed)").
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "echo $$ > {dir}/a.pid; sleep 60")
        + &sh("gated", "true")
        + &order_any_exit("a", "gated")
        + &shutdown_grace_ms(2000);

    let child = spawn_graph_in(dir.path(), &graph);
    let orchestrator_pid = child.id() as i32;

    let a_pid = wait_for_pid(&dir.path().join("a.pid"), Duration::from_secs(10));
    assert!(alive(a_pid), "a should be running before the signal");

    signal::kill(Pid::from_raw(orchestrator_pid), Signal::SIGTERM)
        .expect("send SIGTERM to the orchestrator");

    let out = wait_within(child, Duration::from_secs(10));

    assert_ne!(
        code(&out),
        0,
        "a signalled run must exit nonzero\nstderr:\n{}",
        stderr(&out)
    );
    assert_eq!(
        outcome_of(&out, "gated"),
        "never started (the orchestrator was shut down by a signal)",
        "a node stranded by shutdown must not be rendered through the culprit-node \
         template:\n{}",
        stderr(&out)
    );

    assert_gone(a_pid, "a's sleep");
}

#[test]
fn no_orphans_on_a_normal_run() {
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "echo $$ > {dir}/a.pid")
        + &sh(
            "b",
            "echo $$ > {dir}/b.pid; sleep 1; echo $$ >> {dir}/b.pid",
        );

    let out = run_graph_in(dir.path(), &graph);
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));

    let a_pid = wait_for_pid(&dir.path().join("a.pid"), Duration::from_secs(5));
    let b_text = std::fs::read_to_string(dir.path().join("b.pid")).unwrap();
    let b_pid: i32 = b_text
        .lines()
        .next()
        .expect("b wrote its pid")
        .trim()
        .parse()
        .unwrap();

    assert_gone(a_pid, "a");
    assert_gone(b_pid, "b");
}
