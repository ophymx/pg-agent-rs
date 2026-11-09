//! Is pgpool serving the certificate that is on disk?
//!
//! pgpool loads its TLS material at start and never again; a renewer
//! that rewrites `ssl_cert` and reloads (or does nothing) leaves pgpool
//! presenting the old certificate until something restarts it. From
//! 2026-10-19 the home fleet's pgpool served an expired certificate for
//! two weeks: every `sslmode=verify-full` client was refused, every
//! health check passed (plain HTTP, never looked), and the only process
//! on the host that could have compared the two — the agent — did not.
//!
//! [`probe`] reads the certificate pgpool serves on its frontend port
//! and the first certificate in the `ssl_cert` file, and reports both
//! expiries and whether they differ. `stale` is the actionable verdict:
//! the file is a DIFFERENT certificate with a LATER expiry than the one
//! served — renewed on disk, not loaded. The supervisor restarts pgpool
//! on it; `/healthz` carries the whole status for alerting on
//! `served_not_after` directly.

use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Where to look: pgpool's frontend address and its `ssl_cert` file,
/// both from `pgpool.conf`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgpoolTlsSpec {
    pub addr: SocketAddr,
    pub cert_path: PathBuf,
}

impl PgpoolTlsSpec {
    /// Derive from a parsed `pgpool.conf`: `None` unless `ssl = on` and
    /// `ssl_cert` is set. The frontend is dialled on loopback —
    /// `listen_addresses` is honoured when it names one concrete
    /// address, which is the only case where loopback might not answer.
    pub fn from_conf(conf: &pg_agent_hookspec::PgpoolConf, conf_dir: &Path) -> Option<Self> {
        if !matches!(
            conf.get("ssl"),
            Some("on") | Some("true") | Some("yes") | Some("1")
        ) {
            return None;
        }
        let cert = conf.get("ssl_cert").filter(|c| !c.is_empty())?;
        let cert_path = {
            let p = PathBuf::from(cert);
            if p.is_absolute() {
                p
            } else {
                conf_dir.join(p)
            }
        };
        let port: u16 = conf
            .get("port")
            .and_then(|p| p.parse().ok())
            .unwrap_or(9999);
        let ip = conf
            .get("listen_addresses")
            .and_then(|l| l.split(',').next())
            .map(str::trim)
            .and_then(|l| l.parse::<std::net::IpAddr>().ok())
            .filter(|ip| !ip.is_unspecified())
            .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        Some(Self {
            addr: SocketAddr::new(ip, port),
            cert_path,
        })
    }
}

/// One side of the comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertFacts {
    /// Hex SHA-256 over the DER leaf.
    pub fingerprint: String,
    pub not_after: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct PgpoolTlsStatus {
    /// What pgpool presented; `None` with `error` when it could not be
    /// read (pgpool down, SSL off, handshake failed).
    pub served: Option<CertFacts>,
    /// What `ssl_cert` holds; `None` with `error` when unreadable.
    pub on_disk: Option<CertFacts>,
    /// Both read, different, and the file expires later than what is
    /// served: renewed on disk, not loaded. The supervisor acts on this.
    pub stale: bool,
    /// Both read and identical.
    pub served_matches_disk: bool,
    pub error: Option<String>,
}

/// Compare what pgpool serves with what is on disk.
pub async fn probe(spec: &PgpoolTlsSpec, timeout: Duration) -> PgpoolTlsStatus {
    let mut status = PgpoolTlsStatus::default();
    let mut errors = Vec::new();
    match tokio::fs::read(&spec.cert_path).await {
        Ok(pem) => match first_certificate(&pem) {
            Ok(der) => match facts(&der) {
                Ok(f) => status.on_disk = Some(f),
                Err(e) => errors.push(format!("{}: {e}", spec.cert_path.display())),
            },
            Err(e) => errors.push(format!("{}: {e}", spec.cert_path.display())),
        },
        Err(e) => errors.push(format!("read {}: {e}", spec.cert_path.display())),
    }
    match crate::pgwire_probe::fetch_served_certificate(spec.addr, "localhost", timeout).await {
        Ok(chain) => match facts(&chain[0]) {
            Ok(f) => status.served = Some(f),
            Err(e) => errors.push(format!("served certificate: {e}")),
        },
        Err(e) => errors.push(format!("served certificate: {e}")),
    }
    if let (Some(s), Some(d)) = (&status.served, &status.on_disk) {
        status.served_matches_disk = s.fingerprint == d.fingerprint;
        status.stale = !status.served_matches_disk && d.not_after > s.not_after;
    }
    if !errors.is_empty() {
        status.error = Some(errors.join("; "));
    }
    status
}

fn first_certificate(pem: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut slice = pem;
    let first = rustls_pemfile::certs(&mut slice)
        .next()
        .ok_or_else(|| anyhow::anyhow!("no CERTIFICATE block"))??;
    Ok(first.as_ref().to_vec())
}

fn facts(der: &[u8]) -> anyhow::Result<CertFacts> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let (_, cert) =
        X509Certificate::from_der(der).map_err(|e| anyhow::anyhow!("parse x509: {e}"))?;
    let ts = cert.validity().not_after.timestamp();
    let not_after = Utc
        .timestamp_opt(ts, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("not_after out of range: {ts}"))?;
    let mut h = Sha256::new();
    h.update(der);
    Ok(CertFacts {
        fingerprint: hex::encode(h.finalize()),
        not_after,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_agent_hookspec::PgpoolConf;

    #[test]
    fn spec_requires_ssl_on_and_a_cert() {
        let dir = Path::new("/etc/pgpool2");
        assert!(
            PgpoolTlsSpec::from_conf(&PgpoolConf::parse("ssl = off\nssl_cert = 'a.pem'"), dir)
                .is_none()
        );
        assert!(PgpoolTlsSpec::from_conf(&PgpoolConf::parse("ssl = on"), dir).is_none());
        let s = PgpoolTlsSpec::from_conf(
            &PgpoolConf::parse(
                "ssl = on\nssl_cert = 'server.crt'\nport = 5433\nlisten_addresses = '*'",
            ),
            dir,
        )
        .unwrap();
        assert_eq!(s.addr, "127.0.0.1:5433".parse().unwrap());
        assert_eq!(s.cert_path, PathBuf::from("/etc/pgpool2/server.crt"));
    }

    #[test]
    fn spec_honours_a_concrete_listen_address() {
        let s = PgpoolTlsSpec::from_conf(
            &PgpoolConf::parse("ssl = on\nssl_cert = '/x.pem'\nlisten_addresses = '10.0.0.5'"),
            Path::new("/etc/pgpool2"),
        )
        .unwrap();
        assert_eq!(s.addr, "10.0.0.5:9999".parse().unwrap());
    }

    #[test]
    fn stale_means_different_and_later_on_disk() {
        let mk = |fp: &str, year: i32| CertFacts {
            fingerprint: fp.into(),
            not_after: Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap(),
        };
        let judge = |served: CertFacts, disk: CertFacts| {
            let mut st = PgpoolTlsStatus {
                served: Some(served),
                on_disk: Some(disk),
                ..Default::default()
            };
            if let (Some(s), Some(d)) = (&st.served, &st.on_disk) {
                st.served_matches_disk = s.fingerprint == d.fingerprint;
                st.stale = !st.served_matches_disk && d.not_after > s.not_after;
            }
            st
        };
        assert!(judge(mk("a", 2026), mk("b", 2027)).stale);
        assert!(!judge(mk("a", 2026), mk("a", 2026)).stale);
        // A different but OLDER file is not something to load.
        assert!(!judge(mk("a", 2027), mk("b", 2026)).stale);
    }

    #[tokio::test]
    async fn probe_reports_both_failures_without_panicking() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let st = probe(
            &PgpoolTlsSpec {
                addr,
                cert_path: PathBuf::from("/nonexistent/server.crt"),
            },
            Duration::from_secs(1),
        )
        .await;
        assert!(st.served.is_none());
        assert!(st.on_disk.is_none());
        assert!(!st.stale);
        let err = st.error.unwrap();
        assert!(err.contains("nonexistent"), "{err}");
        assert!(err.contains("served certificate"), "{err}");
    }
}
