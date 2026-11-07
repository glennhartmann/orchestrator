//! End-to-end runs of the real binary: concurrency, streaming, failure
//! propagation, and the summary.

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::*;

/// Polls `path` until it contains `needle`, returning how long that took.
/// `None` if it never did within `timeout`.
fn wait_for_contents(path: &Path, needle: &str, timeout: Duration) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(text) = std::fs::read_to_string(path)
            && text.contains(needle)
        {
            return Some(start.elapsed());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap_or_else(|e| panic!("reading {name}: {e}"))
}

#[test]
fn parallel_nodes_run_in_parallel() {
    // Three nodes with nothing between them. Run sequentially this takes
    // three seconds; the threshold is generous but well under that. Each
    // node leaves a file behind so that "fast" cannot mean "never ran".
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "sleep 1; printf a > {dir}/a.out")
        + &sh("b", "sleep 1; printf b > {dir}/b.out")
        + &sh("c", "sleep 1; printf c > {dir}/c.out");

    let start = Instant::now();
    let out = run_graph_in(dir.path(), &graph);
    let elapsed = start.elapsed();

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    for name in ["a.out", "b.out", "c.out"] {
        assert_eq!(read(dir.path(), name), name[..1].to_string());
    }
    assert!(
        elapsed < Duration::from_secs(2),
        "three independent `sleep 1` nodes took {elapsed:?}; they did not run concurrently"
    );
}

#[test]
fn downstream_streams_before_upstream_exits() {
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "printf early; sleep 3") + &sh("b", "cat > {dir}/b.out") + &data("a", "b");

    let start = Instant::now();
    let child = spawn_graph_in(dir.path(), &graph);
    let observed = wait_for_contents(
        &dir.path().join("b.out"),
        "early",
        Duration::from_millis(2000),
    );
    let out = wait_within(child, Duration::from_secs(30));
    let total = start.elapsed();

    let observed = observed.expect("b never wrote a's bytes while a was still running");
    assert!(
        observed < Duration::from_millis(1500),
        "b's output only appeared after {observed:?}, which is not streaming"
    );
    // The producer sleeps three seconds after printing, so the run cannot
    // have finished before the bytes were observed: this proves b emitted
    // output *before* a exited, not merely that it eventually did.
    assert!(
        total > Duration::from_millis(2500),
        "the run finished in {total:?}, so a had already exited when b's \
         output appeared and nothing about streaming was proven"
    );
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert_eq!(read(dir.path(), "b.out"), "early");
}

#[test]
fn diamond_end_to_end() {
    // The spec's example: a fans out to b, c and d; e takes c's data and
    // waits on b and d. b, c and d each sleep a second, so running them one
    // after another would take three.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "printf hello")
        + &sh("b", "cat > /dev/null; sleep 1")
        + &sh("c", "cat; sleep 1")
        + &sh("d", "cat > /dev/null; sleep 1")
        + &sh("e", "cat > {dir}/e.out")
        + &data("a", "b")
        + &data("a", "c")
        + &data("a", "d")
        + &data("c", "e")
        + &order("b", "e")
        + &order("d", "e");

    let start = Instant::now();
    let out = run_graph_in(dir.path(), &graph);
    let elapsed = start.elapsed();

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert_eq!(
        read(dir.path(), "e.out"),
        "hello",
        "e must have received a's bytes relayed through c"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "the diamond took {elapsed:?}; b, c and d did not run concurrently"
    );
}

#[test]
fn order_edge_blocks_a_node_whose_data_is_already_flowing() {
    // c's bytes are available to e immediately, but e has an order edge
    // from b and must not start until b exits. Each node appends a line to
    // one shared log, so the ordering the nodes themselves observed is the
    // evidence.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh(
        "c",
        "printf \"c-start\\n\" >> {dir}/log; printf hello; sleep 1",
    ) + &sh("b", "sleep 1; printf \"b-exit\\n\" >> {dir}/log")
        + &sh("e", "printf \"e-start\\n\" >> {dir}/log; cat > {dir}/e.in")
        + &data("c", "e")
        + &order("b", "e");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    let log = read(dir.path(), "log");
    assert_eq!(
        log.lines().collect::<Vec<_>>(),
        vec!["c-start", "b-exit", "e-start"],
        "e started before b exited, even though an order edge forbids it"
    );
    assert_eq!(
        read(dir.path(), "e.in"),
        "hello",
        "the bytes c produced while e was blocked must still reach e"
    );
}

#[test]
fn skipping_a_node_releases_the_producer_its_queue_was_throttling() {
    // `blocked` is held back by an order edge while `p` pours a megabyte at
    // it through a 1 KiB queue, so `p` is throttled by a queue nobody will
    // ever drain. When `bad` fails and `blocked` is skipped, that sink must
    // be cancelled: otherwise `p` is wedged forever and the run never ends
    // — the deadlock the design calls out.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("p", "seq 1 200000")
        + &sh("bad", "sleep 1; exit 1")
        + &sh("blocked", "cat > {dir}/blocked.out")
        + &data("p", "blocked")
        + &data_to_file("p", "{dir}/p.out")
        + &order("bad", "blocked")
        + &queue_cap_bytes(1024);

    let out = wait_within(spawn_graph_in(dir.path(), &graph), Duration::from_secs(60));

    assert_eq!(outcome_of(&out, "blocked"), "skipped (bad failed)");
    assert!(!dir.path().join("blocked.out").exists());
    // The surviving file sink proves the producer was released and ran to
    // completion, rather than merely being killed to break the deadlock.
    assert_eq!(
        read(dir.path(), "p.out").lines().count(),
        200000,
        "the released producer must still deliver its whole stream"
    );
    assert_eq!(outcome_of(&out, "p"), "succeeded");
    assert_eq!(code(&out), 1);
}

#[test]
fn a_producer_whose_consumer_was_already_skipped_still_finishes() {
    // `c` is skipped at once (its order edge comes from `bad`), but its
    // producer `p` only starts a second later, behind `gate`. By then there
    // is nobody to hand a stdin pipe to, so `p`'s fan-out must release that
    // sink as it attaches it — otherwise it waits forever for a pipe that
    // will never arrive and the run never ends.
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("bad", &["false"])
        + &sh("gate", "sleep 1")
        // Run without a shell: `sh -c` would be the process we SIGPIPE, and
        // only some shells exec a lone command. One that forks instead
        // survives to report its child's death as exit 141, which is not the
        // signal death this test is about.
        + &argv("p", &["seq", "1", "100000"])
        + &sh("c", "cat > {dir}/c.out")
        + &order("gate", "p")
        + &data("p", "c")
        + &order("bad", "c");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(outcome_of(&out, "c"), "skipped (bad failed)");
    assert!(!dir.path().join("c.out").exists());
    // Nothing is left to read p's output, so the orchestrator closes the
    // pipe and p dies of SIGPIPE. That is reported as a failure, not as
    // abandonment: c did not walk out, we released its queue on `bad`'s
    // account. Erring this way keeps us from ever excusing a nonzero exit
    // we caused — the run is already failing because c was skipped.
    assert_eq!(outcome_of(&out, "p"), "failed (killed by signal 13)");
    assert_eq!(code(&out), 1);
}

#[test]
fn unrelated_branches_complete_when_one_node_fails() {
    // `bad` fails immediately while `good` is still running. The parallel
    // branch must finish and write its file regardless.
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("bad", &["false"]) + &sh("good", "sleep 1; printf ok > {dir}/good.out");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(read(dir.path(), "good.out"), "ok");
    assert_eq!(outcome_of(&out, "good"), "succeeded");
    assert_eq!(outcome_of(&out, "bad"), "failed (exit 1)");
    assert_eq!(code(&out), 1);
}

#[test]
fn unstarted_dependents_are_skipped_not_run_empty() {
    let dir = tempfile::tempdir().unwrap();
    let graph =
        argv("bad", &["false"]) + &sh("dep", "printf ran > {dir}/dep.out") + &order("bad", "dep");

    let out = run_graph_in(dir.path(), &graph);

    assert!(
        !dir.path().join("dep.out").exists(),
        "a skipped node must never execute, not even with empty input"
    );
    assert_eq!(outcome_of(&out, "dep"), "skipped (bad failed)");
    assert_eq!(code(&out), 1);
}

#[test]
fn skipping_propagates_transitively() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("bad", &["false"])
        + &sh("mid", "printf ran > {dir}/mid.out")
        + &sh("last", "printf ran > {dir}/last.out")
        + &order("bad", "mid")
        + &order("mid", "last");

    let out = run_graph_in(dir.path(), &graph);

    assert!(!dir.path().join("mid.out").exists());
    assert!(
        !dir.path().join("last.out").exists(),
        "a dependent of a skipped node must itself be skipped"
    );
    assert_eq!(outcome_of(&out, "mid"), "skipped (bad failed)");
    assert_eq!(outcome_of(&out, "last"), "skipped (bad failed)");
    assert_eq!(code(&out), 1);
}

#[test]
fn any_exit_order_edge_runs_after_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("bad", &["false"])
        + &sh("cleanup", "printf cleaned > {dir}/cleanup.out")
        + &order_any_exit("bad", "cleanup");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(
        read(dir.path(), "cleanup.out"),
        "cleaned",
        "an any-exit order edge is exactly the cleanup-node case: it must run"
    );
    assert_eq!(outcome_of(&out, "cleanup"), "succeeded");
    assert_eq!(code(&out), 1, "the failure itself still fails the run");
}

#[test]
fn running_dependent_finishes_with_truncated_input() {
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("p", "printf partial; exit 1") + &sh("c", "cat > {dir}/c.out") + &data("p", "c");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(
        read(dir.path(), "c.out"),
        "partial",
        "the already-running consumer keeps the bytes that did arrive"
    );
    assert_eq!(outcome_of(&out, "p"), "failed (exit 1)");
    assert_eq!(
        outcome_of(&out, "c"),
        "exited 0 with truncated input (p failed mid-stream)"
    );
    assert_eq!(code(&out), 1, "truncated results make the run a failure");
}

#[test]
fn a_failed_producer_is_not_excused_as_abandoned() {
    // `p` writes more than its consumer's 1 KiB queue (but less than a pipe
    // buffer, so it can still exit) and then fails. `c` is held back by an
    // order edge, so it is skipped — and cancelling its queue is what
    // leaves `p`'s fan-out with no sinks at all. That must not be read as
    // "its last consumer went away": the consumer went away *because p
    // failed*, and calling it abandonment would excuse the failure.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("p", "seq 1 5000; exit 1")
        + &sh("b", "sleep 2")
        + &sh("c", "cat > {dir}/c.out")
        + &data("p", "c")
        + &order("b", "c")
        + &queue_cap_bytes(1024);

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(outcome_of(&out, "p"), "failed (exit 1)");
    assert_eq!(outcome_of(&out, "c"), "skipped (p failed)");
    assert!(!dir.path().join("c.out").exists());
    assert_eq!(code(&out), 1);
}

#[test]
fn missing_program_is_reported_as_spawn_failure() {
    let dir = tempfile::tempdir().unwrap();
    let graph = argv("missing", &["orchestrator-no-such-program-xyzzy"])
        + &sh("dep", "printf ran > {dir}/dep.out")
        + &order("missing", "dep");

    let out = run_graph_in(dir.path(), &graph);

    let missing = outcome_of(&out, "missing");
    assert!(
        missing.starts_with("failed to start:"),
        "a missing program is a node failure, not a crash: {missing}"
    );
    assert_eq!(outcome_of(&out, "dep"), "skipped (missing failed)");
    assert!(!dir.path().join("dep.out").exists());
    assert_eq!(code(&out), 1);
}

#[test]
fn consumer_that_exits_early_does_not_disturb_siblings() {
    // `head -1` leaves after one line; its sibling must still receive the
    // whole stream, and the producer must still finish successfully.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "seq 1 20000")
        + &argv("h", &["head", "-1"])
        + &sh("c", "cat > {dir}/c.out")
        + &data("a", "h")
        + &data("a", "c");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(
        read(dir.path(), "c.out").lines().count(),
        20000,
        "the surviving consumer must receive every line"
    );
    assert_eq!(outcome_of(&out, "a"), "succeeded");
    assert_eq!(outcome_of(&out, "h"), "succeeded");
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
}

#[test]
fn producer_killed_by_sigpipe_is_abandoned_not_failed() {
    // `yes | head -1`: once the only consumer is gone the orchestrator
    // closes its read end and the producer dies of SIGPIPE. That is not a
    // graph failure.
    let graph = argv("y", &["yes"]) + &argv("h", &["head", "-1"]) + &data("y", "h");

    let out = run_graph(&graph);

    assert!(
        outcome_of(&out, "y").starts_with("abandoned"),
        "got {:?}\nstderr:\n{}",
        outcome_of(&out, "y"),
        stderr(&out)
    );
    assert_eq!(outcome_of(&out, "h"), "succeeded");
    assert!(
        stdout(&out).contains("[h] y"),
        "the consumer's own output still passes through: {:?}",
        stdout(&out)
    );
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
}

#[test]
fn a_data_edge_that_names_no_stream_takes_stdout() {
    // `stream` left out of the edge is the proto's default, zero, i.e.
    // STREAM_UNSPECIFIED. It must behave as an ordinary stdout edge: the
    // bytes reach the consumer and stdout counts as claimed, so nothing
    // passes through and no diagnostic is warranted.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "printf hello")
        + &sh("b", "cat > {dir}/b.out")
        + "[[edges]]\n[edges.data]\nnodeStdin = \"b\"\n\n[edges.data.node]\nnode = \"a\"\n\n";

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(read(dir.path(), "b.out"), "hello");
    assert_eq!(stdout(&out), "", "the edge claimed a's stdout");
    let err = stderr(&out);
    assert!(!err.contains("note:"), "nothing to diagnose here:\n{err}");
    assert_eq!(code(&out), 0, "stderr:\n{err}");
}

#[test]
fn a_node_sigpiped_with_its_sinks_intact_fails_and_blocks_its_dependents() {
    // SIGPIPE from somewhere other than a vanished consumer — here the node
    // signals itself — is an ordinary failure: nothing had stopped reading
    // its output. What must not happen is the summary calling it failed
    // while its dependents ran anyway, as a SIGPIPE-means-abandoned
    // shortcut would.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("sig", "kill -PIPE $$")
        + &sh("dep", "printf ran > {dir}/dep.out")
        + &order("sig", "dep");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(outcome_of(&out, "sig"), "failed (killed by signal 13)");
    assert_eq!(
        outcome_of(&out, "dep"),
        "skipped (sig failed)",
        "the summary calls sig a failure, so its dependents must be treated \
         as a failure's dependents"
    );
    assert!(!dir.path().join("dep.out").exists());
    assert_eq!(code(&out), 1);
}

#[test]
fn a_node_that_exits_nonzero_after_losing_its_last_consumer_is_abandoned() {
    // The converse: the consumer leaves, and the producer notices EPIPE and
    // exits nonzero under its own steam rather than dying of SIGPIPE (it
    // ignores the signal). It is abandoned, so it counts as success — and
    // its order dependent must therefore run, not be skipped.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("p", "trap \"\" PIPE; seq 1 200000; exit 7")
        + &argv("h", &["head", "-1"])
        + &sh("dep", "printf ran > {dir}/dep.out")
        + &data("p", "h")
        + &order("p", "dep");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(
        outcome_of(&out, "p"),
        "abandoned (its last consumer went away)"
    );
    assert_eq!(
        read(dir.path(), "dep.out"),
        "ran",
        "abandonment counts as success, so the dependent must have run"
    );
    assert_eq!(outcome_of(&out, "dep"), "succeeded");
    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
}

#[test]
fn a_broken_orchestrator_stdout_does_not_excuse_a_failing_node() {
    // Being piped into something that leaves early — `orchestrator run g |
    // head -1` — is ordinary usage. The pass-through sink going away is the
    // orchestrator's own plumbing, not the graph walking out, so it must
    // not turn a node's failure into a successful run.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "seq 1 200000; exit 1");

    let out = run_graph_with_broken_stdout(dir.path(), &graph);

    assert_eq!(outcome_of(&out, "a"), "failed (exit 1)");
    assert_eq!(
        code(&out),
        1,
        "a failing node is a failing run even when our own stdout is gone"
    );
}

#[test]
fn a_node_we_sigpiped_because_our_own_stdout_went_away_is_not_blamed() {
    // `orchestrator run g | head -1`. Nothing the graph asked for went
    // wrong: the node's work completed, and the only departed consumer was
    // the orchestrator's own stdout. Reporting it as "failed (killed by
    // signal 13)" blames a blameless node for ordinary `| head` usage.
    let dir = tempfile::tempdir().unwrap();
    // No shell: see the note in
    // `a_producer_whose_consumer_was_already_skipped_still_finishes`.
    let graph = argv("a", &["seq", "1", "200000"]);

    let out = run_graph_with_broken_stdout(dir.path(), &graph);

    assert_eq!(
        outcome_of(&out, "a"),
        "stopped when the orchestrator's stdout went away (killed by signal 13)"
    );
    let err = stderr(&out);
    assert!(
        err.contains(
            "note: stdout was closed from our end once the orchestrator's stdout stopped \
             accepting writes"
        ),
        "the summary must say why the node died:\n{err}"
    );
    assert_eq!(
        code(&out),
        0,
        "every node's work completed and only our own terminal left:\n{err}"
    );
}

// `/dev/full` is Linux-only; see the note above
// `a_destination_that_fails_mid_stream_fails_the_run`.
#[cfg(target_os = "linux")]
#[test]
fn a_write_failure_on_our_own_stdout_is_still_a_failing_run() {
    // `orchestrator run g > /dev/full`. The pass-through sink leaves the
    // live set exactly as a departed `head` makes it, so the node takes our
    // SIGPIPE either way — but here the output was *lost*, not merely no
    // longer wanted, and a script reading only the exit code must not be
    // told the run succeeded.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "seq 1 200000");

    let out = run_graph_with_stdout_to(dir.path(), &graph, "/dev/full");

    let outcome = outcome_of(&out, "a");
    assert!(
        outcome.contains("the orchestrator's stdout") && outcome.contains("No space left"),
        "the failed destination must be named in the node's own outcome: {outcome}"
    );
    assert_eq!(
        code(&out),
        1,
        "output thrown away makes the run wrong:\n{}",
        stderr(&out)
    );
}

#[test]
fn another_nodes_failure_cancelling_a_sink_does_not_excuse_this_node() {
    // m fails, which skips c, which releases the sink c was being fed
    // through — and that sink was n's only destination. n then exits nonzero
    // under its own steam (it ignores SIGPIPE). n's audience did not walk
    // out, we shut it on m's account, so n is a failure and its own
    // dependent must be skipped. Timing cannot tell this case apart: the
    // cancellation belongs to another node and lands before n exits.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("m", "sleep 1; exit 1")
        + &sh("n", "trap \"\" PIPE; seq 1 200000; exit 5")
        + &sh("c", "cat > {dir}/c.out")
        + &sh("dep", "printf ran > {dir}/dep.out")
        + &data("n", "c")
        + &order("m", "c")
        + &order("n", "dep")
        + &queue_cap_bytes(1024);

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(outcome_of(&out, "c"), "skipped (m failed)");
    assert_eq!(
        outcome_of(&out, "n"),
        "failed (exit 5)",
        "n lost its sink to our cancellation, which is no excuse for exiting 5"
    );
    assert_eq!(outcome_of(&out, "dep"), "skipped (n failed)");
    assert!(!dir.path().join("dep.out").exists());
    assert_eq!(code(&out), 1);
}

// `/dev/full` accepts an open and then fails every write with ENOSPC, which
// is the only way to get a real mid-stream sink failure without faking I/O.
// It is Linux-only; macOS has no equivalent.
#[cfg(target_os = "linux")]
#[test]
fn a_destination_that_fails_mid_stream_fails_the_run() {
    // The node itself is faultless and exits 0, but its output went on the
    // floor, so the run's results are wrong — the same grounds as truncated
    // input.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "printf x") + &data_to_file("a", "/dev/full");

    let out = run_graph_in(dir.path(), &graph);

    let outcome = outcome_of(&out, "a");
    assert!(
        outcome.starts_with("its output to /dev/full failed:"),
        "the lost destination must be named in the node's own outcome: {outcome}"
    );
    let err = stderr(&out);
    assert!(
        err.contains("note:") && err.contains("/dev/full"),
        "and noted alongside it:\n{err}"
    );
    assert_eq!(
        code(&out),
        1,
        "output thrown away makes the run wrong even though the node exited 0"
    );
}

#[test]
fn unredirected_output_is_tagged_on_the_right_stream() {
    let graph = sh("n", "echo o; echo e >&2");

    let out = run_graph(&graph);

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        "[n] o\n",
        "a node's stdout passes through to the orchestrator's stdout, tagged"
    );
    let err = stderr(&out);
    assert!(
        err.contains("[n] e\n"),
        "a node's stderr passes through to the orchestrator's stderr, tagged: {err:?}"
    );
    assert!(
        err.contains("run summary:"),
        "the summary shares stderr: {err:?}"
    );
}

#[test]
fn summary_goes_to_stderr_and_stdout_stays_clean() {
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "printf x") + &data_to_file("a", "{dir}/a.out");

    let out = run_graph_in(dir.path(), &graph);

    assert_eq!(read(dir.path(), "a.out"), "x");
    assert_eq!(
        stdout(&out),
        "",
        "with every stream redirected, stdout must carry nothing at all"
    );
    assert!(stderr(&out).contains("run summary:"));
    assert_eq!(code(&out), 0);
}

#[test]
fn a_consumer_that_fails_to_start_releases_the_producer_it_was_throttling() {
    // The mirror image of a skip: the consumer will never read its stdin,
    // so the queue filling for it must be released when its *own* spawn
    // fails. Otherwise its sink waits forever for a stdin pipe that cannot
    // arrive, holding the permits the fan-out needs, and the producer —
    // throttled by a 1 KiB queue — never exits and the run never ends.
    let dir = tempfile::tempdir().unwrap();
    // No shell: see the note in
    // `a_producer_whose_consumer_was_already_skipped_still_finishes`.
    let graph = argv("p", &["seq", "1", "200000"])
        + &argv("c", &["orchestrator-no-such-program-xyzzy"])
        + &data("p", "c")
        + &queue_cap_bytes(1024);

    let out = run_graph_in(dir.path(), &graph);

    assert!(
        outcome_of(&out, "c").starts_with("failed to start:"),
        "got {:?}",
        outcome_of(&out, "c")
    );
    // Nothing is left to read p's output, so the orchestrator closes the
    // pipe and p dies of SIGPIPE. It is *not* abandoned: the sink went away
    // because we released it, not because a consumer walked out.
    assert_eq!(outcome_of(&out, "p"), "failed (killed by signal 13)");
    assert_eq!(code(&out), 1);
}

#[test]
fn an_unopenable_file_alongside_a_consumer_in_one_fanout_still_reports() {
    // One fan-out with two destinations: a consumer's stdin *and* a file
    // that cannot be opened. Building the fan-out fails on the file after
    // the consumer has already been registered against it, so a scheduler
    // that tracked the consumer by an index into a list it then never
    // appended to would panic here instead of reporting anything.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("p", "printf hi")
        + &sh("c", "cat > {dir}/c.out")
        + &data("p", "c")
        + &data_to_file("p", "{dir}");

    let out = run_graph_in(dir.path(), &graph);

    let outcome = outcome_of(&out, "p");
    assert!(
        outcome.starts_with("failed to start:") && outcome.contains("cannot reach"),
        "got {outcome}"
    );
    assert_eq!(outcome_of(&out, "c"), "skipped (p failed)");
    assert!(!dir.path().join("c.out").exists());
    assert_eq!(code(&out), 1);
}

#[test]
fn an_unopenable_file_in_a_second_fanout_still_reports() {
    // The same shape, but the failing fan-out is the node's *second*
    // (stderr): its consumer is registered after a first, successful
    // fan-out, so an index recorded for it points one past the end rather
    // than at nothing. Both shapes have to be safe, not just the first.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("p", "printf hi")
        + &sh("c", "cat > {dir}/c.out")
        + &sh("d", "cat > {dir}/d.out")
        + &data("p", "c")
        + &data_stderr("p", "d")
        + &data_stderr_to_file("p", "{dir}");

    let out = run_graph_in(dir.path(), &graph);

    let outcome = outcome_of(&out, "p");
    assert!(
        outcome.starts_with("failed to start:") && outcome.contains("cannot reach"),
        "got {outcome}"
    );
    assert_eq!(outcome_of(&out, "c"), "skipped (p failed)");
    assert_eq!(outcome_of(&out, "d"), "skipped (p failed)");
    assert!(!dir.path().join("c.out").exists());
    assert!(!dir.path().join("d.out").exists());
    assert_eq!(code(&out), 1);
}

#[test]
fn a_node_whose_output_sink_cannot_be_opened_does_not_run() {
    // The sink path is an existing directory, so it passes validation (its
    // parent is writable) and only fails when opened. Running the node
    // anyway would send its results nowhere and still call the run a
    // success, so it must not start at all — and its dependents are
    // skipped, exactly as for any other failure to start.
    let dir = tempfile::tempdir().unwrap();
    let graph = sh("a", "printf x > {dir}/ran")
        + &sh("dep", "printf ran > {dir}/dep.out")
        + &data_to_file("a", "{dir}")
        + &order("a", "dep");

    let out = run_graph_in(dir.path(), &graph);

    let outcome = outcome_of(&out, "a");
    assert!(
        outcome.starts_with("failed to start:") && outcome.contains("cannot reach"),
        "an unopenable destination must be named, loudly: {outcome}"
    );
    assert!(
        !dir.path().join("ran").exists(),
        "the node must never have run"
    );
    assert_eq!(outcome_of(&out, "dep"), "skipped (a failed)");
    assert_eq!(code(&out), 1);
}

#[test]
fn empty_graph_succeeds_with_exit_zero() {
    let out = run_graph("# a graph with no nodes at all\n");

    assert_eq!(code(&out), 0, "stderr:\n{}", stderr(&out));
    assert!(
        stderr(&out).contains("(no nodes)"),
        "stderr:\n{}",
        stderr(&out)
    );
    assert_eq!(stdout(&out), "");
}
