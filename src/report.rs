//! Per-node outcomes, the end-of-run summary, and the process exit code.

use std::io;

/// How one node's run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Succeeded,
    /// Died after its last consumer went away — the `yes | head -1` case.
    /// Counts as success, both for scheduling and for the exit code.
    Abandoned,
    /// Exited 0, but its stdin was cut short by an upstream failure, so its
    /// results are wrong even though it is not itself at fault.
    TruncatedInput {
        upstream: String,
    },
    /// One of its destinations failed mid-stream, so some of the output it
    /// produced never arrived. Like [`Self::TruncatedInput`], the node is
    /// not at fault and the run's results are still on the floor.
    ///
    /// Used for a node that exited 0, and for one the orchestrator SIGPIPEd
    /// *because* the failing destination was the last sink of a pass-through
    /// fan-out (`> /dev/full`) — there the lost bytes outrank the fact that
    /// we were the proximate cause of the death, so this wins over
    /// [`Self::PassthroughGone`]. A node that failed on its own account
    /// keeps [`Self::Failed`].
    LostOutput {
        destination: String,
        error: String,
    },
    /// Took SIGPIPE because the *orchestrator's own* stdout or stderr had
    /// gone away — `orchestrator run g | head -1` — so we closed our read
    /// end of this node's stream and it died writing into it.
    ///
    /// Distinct from [`Self::Abandoned`], where the departed consumer was
    /// another node, and from [`Self::Failed`], where the node died on its
    /// own account. Like abandonment it counts as success: every node's
    /// work completed and the only consumer that left was our own terminal,
    /// which is the `yes | head -1` case one level out.
    PassthroughGone {
        destination: String,
    },
    Failed {
        code: Option<i32>,
        signal: Option<i32>,
    },
    SpawnFailed {
        message: String,
    },
    /// Never started, because `because` failed.
    Skipped {
        because: String,
    },
    /// Never started, not because anything failed but because the
    /// orchestrator was shut down (SIGINT/SIGTERM) before scheduling got to
    /// it. Kept separate from [`Self::Skipped`]: that variant's `because`
    /// names a *culprit node* and is rendered into "skipped ({because}
    /// failed)" — a shutdown reason does not fit that template, and
    /// stuffing it in there read as broken English.
    Interrupted,
}

/// What every node did, plus any per-node diagnostics that are not
/// outcomes of their own.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Report {
    /// One entry per node, in plan order.
    pub entries: Vec<(String, Outcome)>,
    /// `(node name, message)` diagnostics that belong to a node but are not
    /// its outcome: a stream that could not be read to the end, a sink that
    /// failed to open or to write. Reported under the node's entry so they
    /// are never silent.
    pub notes: Vec<(String, String)>,
    /// Set once the run received SIGINT or SIGTERM. Forces a nonzero exit
    /// code even in the (racy) case where every node had already succeeded
    /// by the time the signal was handled: the operator asked this run to
    /// stop, so it is not a clean success regardless of what the nodes
    /// themselves report.
    pub interrupted: bool,
}

impl Outcome {
    /// One line of summary prose, naming the status and whatever is needed
    /// to act on it: the exit status, or the node whose failure is to blame.
    fn describe(&self) -> String {
        match self {
            Outcome::Succeeded => "succeeded".to_string(),
            Outcome::Abandoned => "abandoned (its last consumer went away)".to_string(),
            Outcome::TruncatedInput { upstream } => {
                format!("exited 0 with truncated input ({upstream} failed mid-stream)")
            }
            // Deliberately says nothing about *how* the node ended. It
            // covers both a node that exited 0 and one the orchestrator
            // SIGPIPEd once the failing destination left the fan-out with
            // no sinks, and "exited 0 but ..." would be a lie about the
            // second. The node's blamelessness is in the wording's silence
            // about it; the destination and its error are what to act on.
            Outcome::LostOutput { destination, error } => {
                format!("its output to {destination} failed: {error}")
            }
            Outcome::PassthroughGone { destination } => {
                format!("stopped when {destination} went away (killed by signal 13)")
            }
            Outcome::Failed {
                code: Some(code),
                signal: _,
            } => format!("failed (exit {code})"),
            Outcome::Failed {
                code: None,
                signal: Some(signal),
            } => format!("failed (killed by signal {signal})"),
            Outcome::Failed {
                code: None,
                signal: None,
            } => "failed (unknown exit status)".to_string(),
            Outcome::SpawnFailed { message } => format!("failed to start: {message}"),
            Outcome::Skipped { because } => format!("skipped ({because} failed)"),
            Outcome::Interrupted => {
                "never started (the orchestrator was shut down by a signal)".to_string()
            }
        }
    }
}

impl Report {
    /// Writes the human-readable summary. The caller sends this to stderr:
    /// stdout belongs to the nodes' pass-through output.
    pub fn write(&self, w: &mut impl io::Write) -> io::Result<()> {
        if self.interrupted {
            writeln!(w, "run summary (interrupted by signal):")?;
        } else {
            writeln!(w, "run summary:")?;
        }
        if self.entries.is_empty() {
            writeln!(w, "  (no nodes)")?;
        }
        for (name, outcome) in &self.entries {
            writeln!(w, "  {name}: {}", outcome.describe())?;
            for (_, message) in self.notes.iter().filter(|(n, _)| n == name) {
                writeln!(w, "    note: {message}")?;
            }
        }
        Ok(())
    }

    /// `0` if every node succeeded, was abandoned, or died only because the
    /// orchestrator's own stdout/stderr went away, `1` otherwise. A
    /// truncated-input node exited 0 but still makes the *run* a failure,
    /// because its results are wrong. A signalled run is always nonzero,
    /// even if every node happened to have already succeeded.
    pub fn exit_code(&self) -> i32 {
        if self.interrupted {
            return 1;
        }
        let all_good = self.entries.iter().all(|(_, o)| {
            matches!(
                o,
                Outcome::Succeeded | Outcome::Abandoned | Outcome::PassthroughGone { .. }
            )
        });
        if all_good { 0 } else { 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(entries: &[(&str, Outcome)]) -> Report {
        Report {
            entries: entries
                .iter()
                .map(|(n, o)| (n.to_string(), o.clone()))
                .collect(),
            notes: Vec::new(),
            interrupted: false,
        }
    }

    fn summary(report: &Report) -> String {
        let mut buf = Vec::new();
        report
            .write(&mut buf)
            .expect("writing to a Vec cannot fail");
        String::from_utf8(buf).expect("the summary is UTF-8")
    }

    #[test]
    fn exit_code_is_zero_for_success_and_abandonment() {
        assert_eq!(report(&[]).exit_code(), 0, "an empty run is a success");
        assert_eq!(
            report(&[
                ("a", Outcome::Succeeded),
                ("b", Outcome::Abandoned),
                ("c", Outcome::Succeeded),
            ])
            .exit_code(),
            0,
            "a node abandoned once its last consumer went away is not a failure"
        );
    }

    #[test]
    fn a_node_we_sigpiped_for_our_own_departed_stdout_is_not_a_failing_run() {
        let r = report(&[(
            "a",
            Outcome::PassthroughGone {
                destination: "the orchestrator's stdout".to_string(),
            },
        )]);
        assert_eq!(
            r.exit_code(),
            0,
            "the node's work completed; only our own terminal left"
        );
        let text = summary(&r);
        assert!(
            text.contains("the orchestrator's stdout went away"),
            "the summary must name what went away rather than show a bare signal:\n{text}"
        );
        assert!(
            !text.contains("failed"),
            "this death must not read as the node's own failure:\n{text}"
        );
    }

    #[test]
    fn exit_code_is_one_for_failure_skip_or_truncation() {
        let failures = [
            Outcome::Failed {
                code: Some(1),
                signal: None,
            },
            Outcome::Failed {
                code: None,
                signal: Some(9),
            },
            Outcome::SpawnFailed {
                message: "no such file".to_string(),
            },
            Outcome::Skipped {
                because: "a".to_string(),
            },
            Outcome::TruncatedInput {
                upstream: "a".to_string(),
            },
            Outcome::LostOutput {
                destination: "/dev/full".to_string(),
                error: "No space left on device".to_string(),
            },
        ];
        for outcome in failures {
            let r = report(&[("a", Outcome::Succeeded), ("b", outcome.clone())]);
            assert_eq!(
                r.exit_code(),
                1,
                "{outcome:?} must make the whole run fail even alongside a success"
            );
        }
    }

    #[test]
    fn summary_names_the_cause_of_each_skip() {
        let r = report(&[
            (
                "producer",
                Outcome::Failed {
                    code: Some(3),
                    signal: None,
                },
            ),
            (
                "direct",
                Outcome::Skipped {
                    because: "producer".to_string(),
                },
            ),
            (
                "transitive",
                Outcome::Skipped {
                    because: "producer".to_string(),
                },
            ),
        ]);
        let text = summary(&r);

        for line in text.lines().filter(|l| l.contains("skipped")) {
            assert!(
                line.contains("producer"),
                "a skip line must name the failure that caused it: {line:?}"
            );
        }
        assert_eq!(
            text.lines().filter(|l| l.contains("skipped")).count(),
            2,
            "both skipped nodes must appear in the summary:\n{text}"
        );
        assert!(
            text.contains("direct") && text.contains("transitive"),
            "every node must be named in the summary:\n{text}"
        );
        assert!(
            text.contains("exit 3"),
            "a failure must report its exit status:\n{text}"
        );
    }

    #[test]
    fn interrupted_run_exits_nonzero_even_if_every_node_succeeded() {
        let mut r = report(&[("a", Outcome::Succeeded), ("b", Outcome::Succeeded)]);
        assert_eq!(r.exit_code(), 0, "sanity: not interrupted, all succeeded");

        r.interrupted = true;
        assert_eq!(
            r.exit_code(),
            1,
            "a signalled run must exit nonzero even though every node succeeded"
        );
        assert!(
            summary(&r).contains("interrupted"),
            "the summary should say the run was interrupted:\n{}",
            summary(&r)
        );
    }

    #[test]
    fn summary_names_the_destination_that_lost_a_nodes_output() {
        let r = report(&[(
            "a",
            Outcome::LostOutput {
                destination: "/dev/full".to_string(),
                error: "No space left on device (os error 28)".to_string(),
            },
        )]);
        let text = summary(&r);

        assert!(
            text.contains("/dev/full") && text.contains("No space left on device"),
            "a lost destination is only actionable if it is named, with its error:\n{text}"
        );
    }

    #[test]
    fn notes_appear_under_the_node_they_belong_to() {
        let mut r = report(&[("a", Outcome::Succeeded), ("b", Outcome::Succeeded)]);
        r.notes
            .push(("a".to_string(), "reading stdout failed: boom".to_string()));
        let text = summary(&r);

        let lines: Vec<&str> = text.lines().collect();
        let a_line = lines
            .iter()
            .position(|l| l.contains("a:"))
            .expect("node a is in the summary");
        assert!(
            lines[a_line + 1].contains("reading stdout failed: boom"),
            "a note must follow its own node's entry:\n{text}"
        );
    }
}
