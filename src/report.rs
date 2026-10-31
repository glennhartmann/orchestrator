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
    /// Exited 0, but one of its destinations failed mid-stream, so some of
    /// the output it produced never arrived. Like [`Self::TruncatedInput`],
    /// the node is not at fault and the run's results are still on the
    /// floor. Only ever used for a node that exited 0: one that failed on
    /// its own account keeps [`Self::Failed`].
    LostOutput {
        destination: String,
        error: String,
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
            Outcome::LostOutput { destination, error } => {
                format!("exited 0 but its output to {destination} failed: {error}")
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
        }
    }
}

impl Report {
    /// Writes the human-readable summary. The caller sends this to stderr:
    /// stdout belongs to the nodes' pass-through output.
    pub fn write(&self, w: &mut impl io::Write) -> io::Result<()> {
        writeln!(w, "run summary:")?;
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

    /// `0` if every node succeeded or was abandoned, `1` otherwise. A
    /// truncated-input node exited 0 but still makes the *run* a failure,
    /// because its results are wrong.
    pub fn exit_code(&self) -> i32 {
        let all_good = self
            .entries
            .iter()
            .all(|(_, o)| matches!(o, Outcome::Succeeded | Outcome::Abandoned));
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
