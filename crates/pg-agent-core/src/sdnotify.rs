//! Thin wrapper around `sd_notify(3)`. See SPEC §10.4 and §12.
//!
//! # Race-safety invariant (caller contract)
//!
//! [`ready`] MUST NOT be called until **every** listener the daemon
//! intends to serve has been *bound synchronously* — past the point where
//! `bind()` / `listen()` returns successfully. The accept loops can (and
//! should) run concurrently; only the bind step needs to happen first.
//!
//! Once `READY=1` fires, systemd considers `pg_agentd.service` started
//! and immediately starts every unit that ordered `After=pg_agentd.service`.
//! In our deployment **that includes `pgpool2.service`**, whose first
//! action on startup is often to exec `pg_agentc failover …`, which dials
//! `/run/pg_agentd/pg_agentd.sock`. If we sent `READY=1` before binding,
//! pgpool's call hits an absent socket and the hook fails — observable as
//! "pgpool starts a moment too soon, first failover after a reboot dies".
//!
//! # Why this is called out
//!
//! Code review of the Go implementation surfaced this exact bug. The
//! original `Serve()` spawned goroutines that *would* bind the listeners
//! and then *immediately* called `SdNotify(READY)` without waiting. On a
//! cold-boot a slow scheduler could deliver READY before the goroutine
//! actually reached its `Listen()` call. The fix is the same on either
//! runtime: do the bind synchronously, *then* notify, *then* spawn the
//! accept loop on the bound listener.
//!
//! # Dev-mode no-op
//!
//! When `$NOTIFY_SOCKET` is unset (dev runs outside systemd) every call
//! is a silent no-op — the underlying `sd_notify` crate handles that.

use tracing::{debug, warn};

/// Send `READY=1` to the systemd notify socket.
///
/// **Caller contract** (do not relax without reading the module docs):
/// every listener — Unix socket, peer mTLS TCP, `/healthz` HTTPS — must
/// already be **bound** before this call. Subsequent `accept()` loops
/// can be running or queued for spawn; the only constraint is that the
/// listener file descriptors exist in the kernel so an immediate
/// `connect()` from a downstream unit (notably pgpool2) doesn't see
/// "no such file or directory".
pub fn ready() {
    match sd_notify::notify(false, &[sd_notify::NotifyState::Ready]) {
        Ok(()) => debug!("sd_notify: READY=1 sent"),
        Err(e) => warn!(?e, "sd_notify ready failed"),
    }
}

/// Send `STOPPING=1` to the systemd notify socket. Fire as soon as the
/// graceful shutdown phase begins so systemd's `TimeoutStopSec=` is
/// understood to be the relevant deadline.
pub fn stopping() {
    if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]) {
        warn!(?e, "sd_notify stopping failed");
    }
}
