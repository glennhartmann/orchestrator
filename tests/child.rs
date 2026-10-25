//! Behavioral tests for `orchestrator::exec::child::spawn`.

use std::collections::BTreeMap;
use std::io::Write;
use std::process::Stdio;

use orchestrator::exec::child::{spawn, stdio_for};
use orchestrator::graph::{NodeSpec, StdinSource};
use tokio::io::AsyncReadExt;

fn base_spec(command: Vec<&str>) -> NodeSpec {
    NodeSpec {
        name: "n".to_string(),
        command: command.into_iter().map(str::to_string).collect(),
        env: BTreeMap::new(),
        clear_env: false,
        working_dir: None,
        shell: false,
        stdin: StdinSource::Empty,
        order_deps: Vec::new(),
    }
}

async fn read_all_stdout(child: &mut tokio::process::Child) -> String {
    let mut buf = String::new();
    child
        .stdout
        .take()
        .expect("stdout should be piped")
        .read_to_string(&mut buf)
        .await
        .unwrap();
    buf
}

#[tokio::test]
async fn runs_argv_and_captures_stdout() {
    let spec = base_spec(vec!["echo", "hello"]);
    let mut spawned = spawn(&spec, Stdio::null(), true, false, false).unwrap();

    let out = read_all_stdout(&mut spawned.child).await;
    let status = spawned.child.wait().await.unwrap();

    assert!(status.success());
    assert_eq!(out, "hello\n");
}

#[tokio::test]
async fn shell_true_runs_the_command_line_through_sh() {
    let mut spec = base_spec(vec!["echo", "$HOME"]);
    spec.shell = true;
    let mut spawned = spawn(&spec, Stdio::null(), true, false, false).unwrap();

    let out = read_all_stdout(&mut spawned.child).await;
    spawned.child.wait().await.unwrap();

    let expected_home = std::env::var("HOME").unwrap_or_default();
    assert_eq!(out.trim_end(), expected_home);
}

#[tokio::test]
async fn env_overrides_are_applied_and_parent_env_inherited() {
    // SAFETY: test-only, single-threaded tokio test, no other thread reads
    // this env var concurrently.
    unsafe {
        std::env::set_var("ORCH_TEST_PARENT_VAR", "from-parent");
    }

    let mut spec = base_spec(vec![
        "sh",
        "-c",
        "echo $ORCH_TEST_PARENT_VAR $ORCH_TEST_OVERRIDE",
    ]);
    spec.env.insert(
        "ORCH_TEST_OVERRIDE".to_string(),
        "from-override".to_string(),
    );
    let mut spawned = spawn(&spec, Stdio::null(), true, false, false).unwrap();

    let out = read_all_stdout(&mut spawned.child).await;
    spawned.child.wait().await.unwrap();

    assert_eq!(out, "from-parent from-override\n");
}

#[tokio::test]
async fn clear_env_starts_empty() {
    // SAFETY: test-only, single-threaded tokio test.
    unsafe {
        std::env::set_var("ORCH_TEST_SHOULD_NOT_APPEAR", "nope");
    }

    let mut spec = base_spec(vec!["env"]);
    spec.clear_env = true;
    spec.env
        .insert("ONLY_VAR".to_string(), "only-value".to_string());
    let mut spawned = spawn(&spec, Stdio::null(), true, false, false).unwrap();

    let out = read_all_stdout(&mut spawned.child).await;
    spawned.child.wait().await.unwrap();

    assert_eq!(out, "ONLY_VAR=only-value\n");
}

#[tokio::test]
async fn working_dir_is_respected() {
    let dir = tempfile::tempdir().unwrap();
    // Resolve symlinks (e.g. /tmp -> /private/tmp on macOS) so the
    // comparison isn't tripped up by path normalization differences.
    let canonical = std::fs::canonicalize(dir.path()).unwrap();

    let mut spec = base_spec(vec!["pwd"]);
    spec.working_dir = Some(dir.path().to_path_buf());
    let mut spawned = spawn(&spec, Stdio::null(), true, false, false).unwrap();

    let out = read_all_stdout(&mut spawned.child).await;
    spawned.child.wait().await.unwrap();

    assert_eq!(out.trim_end(), canonical.to_string_lossy());
}

#[tokio::test]
async fn merged_puts_stderr_on_the_stdout_pipe() {
    let spec = base_spec(vec!["sh", "-c", "echo o; echo e >&2"]);
    let mut spawned = spawn(&spec, Stdio::null(), true, true, true).unwrap();

    assert!(
        spawned.child.stderr.is_none(),
        "merged mode must not also expose a separate stderr handle"
    );

    let out = read_all_stdout(&mut spawned.child).await;
    spawned.child.wait().await.unwrap();

    assert_eq!(out, "o\ne\n");
}

#[tokio::test]
async fn child_is_in_its_own_process_group() {
    let spec = base_spec(vec!["sleep", "1"]);
    let spawned = spawn(&spec, Stdio::null(), false, false, false).unwrap();

    let our_pgid = nix::unistd::getpgrp();
    assert_ne!(
        spawned.pgid,
        our_pgid.as_raw(),
        "child must be in a different process group than the orchestrator"
    );

    // Confirm the pgid we returned is real: the child's own pgid as seen
    // by the OS matches what spawn() told us.
    let child_pid = nix::unistd::Pid::from_raw(spawned.child.id().unwrap() as i32);
    let actual_pgid = nix::unistd::getpgid(Some(child_pid)).unwrap();
    assert_eq!(spawned.pgid, actual_pgid.as_raw());

    // Clean up: kill the whole group so `sleep 1` doesn't linger.
    let _ = nix::sys::signal::killpg(
        nix::unistd::Pid::from_raw(spawned.pgid),
        nix::sys::signal::Signal::SIGKILL,
    );
}

#[tokio::test]
async fn missing_program_is_an_io_error_not_a_panic() {
    let spec = base_spec(vec!["this-program-does-not-exist-xyz"]);
    let result = spawn(&spec, Stdio::null(), false, false, false);

    assert!(result.is_err());
}

#[tokio::test]
async fn stdin_source_parent_inherits_the_fd() {
    // `stdio_for(&Parent)` must produce an inherited fd, not a forwarding
    // pipe: we prove this by actually redirecting *this test process's*
    // stdin (fd 0) to a temp file holding known bytes, spawning a child
    // with `StdinSource::Parent`, and checking the child receives exactly
    // those bytes with nothing else involved. If `stdio_for` instead
    // returned `Stdio::piped()`, nothing would ever write to that pipe
    // here, and `cat` would see immediate EOF instead of our bytes.
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(b"known-bytes-for-inheritance-check\n")
        .unwrap();
    tmp.flush().unwrap();
    let file_for_stdin = std::fs::File::open(tmp.path()).unwrap();

    // Save the real fd 0 so we can restore it after the test, then dup2
    // the temp file onto fd 0 of this process.
    let saved_stdin = nix::unistd::dup(std::io::stdin()).unwrap();
    nix::unistd::dup2_stdin(file_for_stdin).unwrap();

    let spec = base_spec(vec!["cat"]);
    let stdin = stdio_for(&StdinSource::Parent).unwrap();
    let spawn_result = spawn(&spec, stdin, true, false, false);

    // Restore the test process's real stdin immediately, before doing
    // anything else that might read from fd 0.
    nix::unistd::dup2_stdin(&saved_stdin).unwrap();

    let mut spawned = spawn_result.unwrap();
    let out = read_all_stdout(&mut spawned.child).await;
    spawned.child.wait().await.unwrap();

    assert_eq!(out, "known-bytes-for-inheritance-check\n");
}

#[tokio::test]
async fn stdin_source_empty_gives_immediate_eof() {
    let spec = base_spec(vec!["cat"]);
    let stdin = stdio_for(&StdinSource::Empty).unwrap();
    let mut spawned = spawn(&spec, stdin, true, false, false).unwrap();

    let out = read_all_stdout(&mut spawned.child).await;
    let status = spawned.child.wait().await.unwrap();

    assert!(status.success());
    assert_eq!(out, "");
}
