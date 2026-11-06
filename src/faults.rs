//! Deterministic fault points at the documented durability boundaries (§11).
//!
//! Only builds with the `fault-injection` feature contain any behavior here; otherwise every
//! function is an empty inline stub. Tests activate points through an environment variable:
//!
//! ```text
//! SHARDSYNC_FAULT="<point>[@<n>]:<action>[:<argument>][,...]"
//! ```
//!
//! Actions:
//! - `abort` — terminate the process immediately, as a crash would.
//! - `pause:<gate>` — create `<gate>.reached`, then wait until the file `<gate>` exists.
//! - `delay:<millis>` — sleep, to make storage slower than the network.
//! - `enospc` — return an out-of-space I/O error from the point.
//!
//! `@<n>` fires only on the n-th time the point is reached (1-based); otherwise every hit fires.

use std::io;

#[cfg(feature = "fault-injection")]
mod enabled {
    use std::io;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    #[derive(Debug)]
    enum Action {
        Abort,
        Pause(PathBuf),
        Delay(Duration),
        OutOfSpace,
    }

    #[derive(Debug)]
    struct Rule {
        point: String,
        nth: Option<u64>,
        action: Action,
        hits: AtomicU64,
    }

    fn rules() -> &'static [Rule] {
        static RULES: OnceLock<Vec<Rule>> = OnceLock::new();
        RULES.get_or_init(|| {
            let Ok(spec) = std::env::var("SHARDSYNC_FAULT") else {
                return Vec::new();
            };
            spec.split(',')
                .filter(|part| !part.trim().is_empty())
                .map(|part| parse_rule(part.trim()))
                .collect()
        })
    }

    fn parse_rule(text: &str) -> Rule {
        let mut parts = text.splitn(3, ':');
        let target = parts.next().unwrap_or_default();
        let action = parts.next().unwrap_or_default();
        let argument = parts.next();
        let (point, nth) = match target.split_once('@') {
            Some((point, n)) => (point, Some(n.parse().expect("fault @n must be a number"))),
            None => (target, None),
        };
        let action = match (action, argument) {
            ("abort", None) => Action::Abort,
            ("pause", Some(gate)) => Action::Pause(PathBuf::from(gate)),
            ("delay", Some(ms)) => {
                Action::Delay(Duration::from_millis(ms.parse().expect("delay millis")))
            }
            ("enospc", None) => Action::OutOfSpace,
            other => panic!("unknown fault action {other:?} in {text:?}"),
        };
        Rule {
            point: point.to_string(),
            nth,
            action,
            hits: AtomicU64::new(0),
        }
    }

    fn fires(rule: &Rule) -> bool {
        let hit = rule.hits.fetch_add(1, Ordering::SeqCst) + 1;
        rule.nth.is_none_or(|n| n == hit)
    }

    fn mark_reached(gate: &std::path::Path) {
        let mut reached = gate.as_os_str().to_owned();
        reached.push(".reached");
        let _ = std::fs::write(PathBuf::from(reached), b"");
    }

    pub fn hit(point: &str) -> io::Result<()> {
        for rule in rules().iter().filter(|r| r.point == point) {
            if !fires(rule) {
                continue;
            }
            match &rule.action {
                Action::Abort => {
                    eprintln!("fault injection: aborting at {point}");
                    std::process::abort();
                }
                Action::Pause(gate) => {
                    mark_reached(gate);
                    while !gate.exists() {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                Action::Delay(duration) => std::thread::sleep(*duration),
                Action::OutOfSpace => {
                    return Err(io::Error::from_raw_os_error(
                        rustix::io::Errno::NOSPC.raw_os_error(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn holding(point: &str) -> bool {
        for rule in rules().iter().filter(|r| r.point == point) {
            if let Action::Pause(gate) = &rule.action {
                if gate.exists() {
                    return false;
                }
                mark_reached(gate);
                return true;
            }
        }
        false
    }
}

/// Reaches a named fault point. Returns an error only for the `enospc` action.
#[inline]
pub fn hit(point: &str) -> io::Result<()> {
    #[cfg(feature = "fault-injection")]
    {
        enabled::hit(point)
    }
    #[cfg(not(feature = "fault-injection"))]
    {
        let _ = point;
        Ok(())
    }
}

/// Non-blocking form of a `pause` gate: true while the gate for `point` is closed. The storage
/// worker uses this to stop advancing verification while it keeps answering commands.
#[inline]
pub fn holding(point: &str) -> bool {
    #[cfg(feature = "fault-injection")]
    {
        enabled::holding(point)
    }
    #[cfg(not(feature = "fault-injection"))]
    {
        let _ = point;
        false
    }
}
