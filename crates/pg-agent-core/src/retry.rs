//! Retry-with-delay for fallible async operations.
//!
//! One helper, deliberately small: bounded attempts, fixed delay, every
//! attempt's error preserved in the final message (a retried failure
//! usually fails for two *different* reasons, and the first one is the
//! interesting one). This is also the intended home for the HA loop's
//! `retry_timeout` semantics when that work lands — a budgeted retry
//! window belongs next to a counted one.
//!
//! Value-predicate retries with bespoke per-attempt logging (e.g.
//! `Agent::verify_primary_with_retries_params`, which retries on a
//! specific verdict variant, not on `Err`) stay hand-written — forcing
//! them through this signature would cost more clarity than it saves.

use std::future::Future;
use std::time::Duration;
use tracing::warn;

/// Run `op` up to `attempts` times (min 1), sleeping `delay` between
/// tries, returning the first `Ok`. On exhaustion the returned error
/// lists every attempt's failure.
pub async fn retry_result<T, F, Fut>(
    desc: &str,
    attempts: u32,
    delay: Duration,
    mut op: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let attempts = attempts.max(1);
    let mut errors: Vec<String> = Vec::new();
    for attempt in 1..=attempts {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt < attempts {
                    warn!(desc, attempt, of = attempts, error = %e, "attempt failed; retrying");
                    errors.push(format!("attempt {attempt}: {e}"));
                    tokio::time::sleep(delay).await;
                } else {
                    errors.push(format!("attempt {attempt}: {e}"));
                }
            }
        }
    }
    Err(anyhow::anyhow!(
        "{desc} failed after {attempts} attempts: {}",
        errors.join("; ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn first_success_returns_immediately() {
        let calls = AtomicU32::new(0);
        let out = retry_result("op", 3, Duration::ZERO, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, anyhow::Error>(42) }
        })
        .await
        .unwrap();
        assert_eq!(out, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn succeeds_on_later_attempt() {
        let calls = AtomicU32::new(0);
        let out = retry_result("op", 3, Duration::ZERO, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    anyhow::bail!("boom {n}")
                }
                Ok("ok")
            }
        })
        .await
        .unwrap();
        assert_eq!(out, "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn exhaustion_reports_every_attempt_error() {
        let calls = AtomicU32::new(0);
        let err = retry_result("stop_postgres", 2, Duration::ZERO, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move { Err::<(), _>(anyhow::anyhow!("boom {n}")) }
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("stop_postgres failed after 2 attempts"),
            "{err}"
        );
        assert!(err.contains("attempt 1: boom 0"), "{err}");
        assert!(err.contains("attempt 2: boom 1"), "{err}");
    }

    #[tokio::test]
    async fn zero_attempts_clamps_to_one() {
        let calls = AtomicU32::new(0);
        let _ = retry_result("op", 0, Duration::ZERO, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, anyhow::Error>(()) }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
