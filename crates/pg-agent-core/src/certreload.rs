//! Hot-reloadable mTLS material. SIGHUP re-reads `ca_cert` / `cert` / `key`
//! from disk and swaps the active bundle atomically. Existing connections
//! are not torn down — subsequent handshakes pick up the new material.
//! See SPEC §7.4.

use crate::config::TlsConfig;
use arc_swap::ArcSwapOption;
use std::sync::Arc;

/// Atomically-swappable cert bundle.
#[derive(Debug)]
pub struct CertBundle {
    // TODO(v1): rustls::sign::CertifiedKey + rustls::RootCertStore for the
    // CA pool. Track leaf + CA SHA-256 fingerprints so Reload can report
    // `changed` without diffing PEM bytes.
}

pub struct CertReloader {
    cfg: TlsConfig,
    bundle: ArcSwapOption<CertBundle>,
}

impl CertReloader {
    pub fn new(cfg: TlsConfig) -> anyhow::Result<Self> {
        // TODO(v1): load initial bundle; fail if cfg.is_configured() is false.
        Ok(Self {
            cfg,
            bundle: ArcSwapOption::from(None),
        })
    }

    /// Re-read all three files. Returns `(changed, ())`; on error the prior
    /// bundle is left intact.
    pub fn reload(&self) -> anyhow::Result<bool> {
        // TODO(v1): rustls-pemfile parse, atomic swap, compare fingerprints.
        let _ = &self.cfg;
        Ok(false)
    }

    pub fn current(&self) -> Option<Arc<CertBundle>> {
        self.bundle.load_full()
    }
}

// TODO(v1):
//   - server_config(allowed_peers) → rustls::ServerConfig with
//     RequireAndVerifyClientCert + SAN allowlist callback.
//   - server_only_config() for /healthz (no client cert required).
//   - client_config() with GetClientCertificate-equivalent that loads the
//     current bundle on each handshake (mirroring the Go reloader's pattern).
