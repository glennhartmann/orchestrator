//! Spawning a single node's process: `argv` vs `sh -c`, env overrides,
//! working directory, stdin wiring, merged stdout/stderr, and placing the
//! child in its own process group so the orchestrator — not the terminal —
//! controls signal delivery to it.

use std::io;
use std::process::Stdio;

use nix::unistd::Pid;
use tokio::process::{Child, Command};

use crate::graph::{NodeSpec, StdinSource};

/// A freshly spawned child together with the process-group id the
/// orchestrator must target when delivering signals to it (and everything
/// it may have forked).
pub struct Spawned {
    pub child: Child,
    pub pgid: i32,
}

/// Maps a resolved [`StdinSource`] to the `Stdio` a child should be spawned
/// with.
///
/// `Parent` inherits the orchestrator's own stdin fd directly rather than
/// going through a forwarding pipe, so a node that prompts for input (e.g.
/// a credential) still works when the orchestrator is run on a TTY.
pub fn stdio_for(src: &StdinSource) -> io::Result<Stdio> {
    Ok(match src {
        StdinSource::Empty => Stdio::null(),
        StdinSource::Parent => Stdio::inherit(),
        StdinSource::Node { .. } => Stdio::piped(),
        StdinSource::File(path) => Stdio::from(std::fs::File::open(path)?),
    })
}

/// Spawns `spec` as a child process in its own process group.
///
/// `want_stdout`/`want_stderr` control whether those streams are piped
/// back to the orchestrator at all (a node whose output nothing consumes
/// needs neither). When `merged` is true, the child's stdout and stderr
/// both end up pointing at the same pipe: after stdio setup but before
/// `exec`, `fd 2` (stderr) in the child is made a duplicate of `fd 1`
/// (stdout, already the pipe's write end), so everything the child writes
/// to either stream lands on the one pipe, in the order it was written,
/// and only `child.stdout` carries data — `child.stderr` is left unset.
pub fn spawn(
    spec: &NodeSpec,
    stdin: Stdio,
    want_stdout: bool,
    want_stderr: bool,
    merged: bool,
) -> io::Result<Spawned> {
    let mut cmd = if spec.shell {
        let mut c = Command::new("sh");
        c.arg("-c").arg(spec.command.join(" "));
        c
    } else {
        let (program, args) = spec
            .command
            .split_first()
            .ok_or_else(|| io::Error::other("node has an empty command"))?;
        let mut c = Command::new(program);
        c.args(args);
        c
    };

    if spec.clear_env {
        cmd.env_clear();
    }
    cmd.envs(spec.env.iter());

    if let Some(dir) = &spec.working_dir {
        cmd.current_dir(dir);
    }

    cmd.stdin(stdin);

    // Each child gets its own process group (pgid == its own pid) so
    // signals the orchestrator sends via `killpg` never also land on the
    // orchestrator itself or on unrelated siblings, and so a terminal's
    // own signal broadcast (e.g. Ctrl-C) doesn't double-deliver.
    cmd.process_group(0);

    if merged {
        cmd.stdout(Stdio::piped());
        // Whatever we set stderr to here is irrelevant: the pre_exec
        // closure below overwrites fd 2 with a dup of fd 1 after stdio
        // setup has already happened, so the child never actually writes
        // to this fd.
        cmd.stderr(Stdio::null());
        // SAFETY: the closure only calls dup2(1, 2), which is async-signal-
        // safe and touches no heap/locks, satisfying pre_exec's
        // fork-safety requirements. It runs after stdio redirection (per
        // tokio's docs: "stdio file descriptors ... have successfully
        // been changed" by the time this runs), so fd 1 is already the
        // pipe write end and dup2 makes fd 2 an alias of it.
        unsafe {
            cmd.pre_exec(|| {
                // SAFETY: fd 1 is a valid, open fd at this point (stdio
                // has already been wired up per tokio's pre_exec docs),
                // and we only borrow it for the duration of this call.
                let stdout_fd = std::os::fd::BorrowedFd::borrow_raw(1);
                nix::unistd::dup2_stderr(stdout_fd)
                    .map_err(|e| io::Error::from_raw_os_error(e as i32))
            });
        }
    } else {
        cmd.stdout(if want_stdout {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        cmd.stderr(if want_stderr {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    }

    let child = cmd.spawn()?;
    let pgid = pgid_of(&child)?;
    Ok(Spawned { child, pgid })
}

fn pgid_of(child: &Child) -> io::Result<i32> {
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("child has already been reaped"))?;
    let pgid = nix::unistd::getpgid(Some(Pid::from_raw(pid as i32)))
        .map_err(|e| io::Error::other(format!("getpgid failed: {e}")))?;
    Ok(pgid.as_raw())
}
