//! SIGINT/SIGTERM handling for the running orchestrator.
//!
//! Each child lives in its own process group (see `child::spawn`), so a
//! signal meant for "everything we started" is delivered with `killpg`
//! rather than `kill`: that reaches a child's own forked descendants too,
//! and never bounces back onto the orchestrator or an unrelated sibling.

use std::time::Duration;

use nix::errno::Errno;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use tokio::signal::unix::{SignalKind, signal as unix_signal};
use tokio::time::{Instant, sleep};

/// How often [`shutdown`] re-checks whether every child is gone yet, while
/// waiting out the grace period.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Resolves the first time this process receives SIGINT or SIGTERM.
///
/// Installing the handlers is the only fallible part — it can only fail if
/// the process is somehow out of file descriptors for the self-pipe tokio
/// uses internally, which is as unrecoverable as any other startup error.
pub async fn wait_for_shutdown_signal() {
    let mut term = unix_signal(SignalKind::terminate())
        .expect("installing the SIGTERM handler should never fail");
    let mut int = unix_signal(SignalKind::interrupt())
        .expect("installing the SIGINT handler should never fail");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

/// Whether a process group's leader is still around. A zombie leader (dead
/// but not yet reaped) still answers a signal-0 probe, so this only turns
/// false once it is actually gone — which is exactly when `shutdown`'s
/// caller is free to stop waiting on it.
fn pgroup_alive(pgid: i32) -> bool {
    match signal::kill(Pid::from_raw(pgid), None) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        // An unexpected errno (e.g. EPERM) is treated as "still there": the
        // grace-period loop will simply keep polling it, and the trailing
        // SIGKILL sweep below will try it again regardless.
        Err(_) => true,
    }
}

/// Sends SIGTERM to every live child's process group, then waits for them
/// to exit — but no longer than `grace`. Whatever is still around once the
/// grace period is up gets SIGKILLed.
///
/// This is a bound on patience, not a sleep: if every child is gone in
/// 200ms, this returns in 200ms.
///
/// `children` is `(name, pgid)` per still-running node, for logging; the
/// name is not otherwise used here.
pub async fn shutdown(children: &[(String, i32)], grace: Duration) {
    for (_, pgid) in children {
        // A child that has already exited between the caller taking this
        // snapshot and this signal going out answers ESRCH, which is fine:
        // it is exactly as dead as we wanted it to be.
        let _ = signal::killpg(Pid::from_raw(*pgid), Signal::SIGTERM);
    }

    let deadline = Instant::now() + grace;
    loop {
        if children.iter().all(|(_, pgid)| !pgroup_alive(*pgid)) {
            return;
        }
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        sleep(POLL_INTERVAL.min(deadline - now)).await;
    }

    for (_, pgid) in children {
        if pgroup_alive(*pgid) {
            let _ = signal::killpg(Pid::from_raw(*pgid), Signal::SIGKILL);
        }
    }
}
