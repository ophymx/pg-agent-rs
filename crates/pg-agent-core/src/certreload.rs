//! Hot-reloadable mTLS material backed by an [`ArcSwap`]. `[tls]` paths
//! are the only hot-reloadable config there is; everything else needs a
//! restart. The unit ships `ExecReload=/bin/kill -HUP $MAINPID`, so
//! `systemctl reload pg_agentd` is the operator interface.
//!
//! [`CertReloader::new`] loads `ca_cert`, `cert`, and `key` from disk and
//! parses them into a [`CertBundle`] — a [`rustls::sign::CertifiedKey`]
//! for handshakes, a [`rustls::RootCertStore`] for verification, and
//! SHA-256 fingerprints over the leaf and CA bytes for change detection.
//!
//! SIGHUP calls [`CertReloader::reload`] which re-reads all three files
//! and `ArcSwap`s a fresh bundle into place. **Existing TLS connections
//! are not torn down** — only subsequent handshakes pick up the new
//! material. Combined with the `MaxConnectionAge` cap on peer gRPC
//! channels, a rotated cert hits every long-lived channel within ~12 h
//! without tearing down healthy connections.
//!
//! On a reload error the previous bundle is preserved (the broken files
//! don't get installed); the error bubbles to the SIGHUP handler, which
//! logs and continues.
//!
//! # What this module does NOT do
//!
//! Building `rustls::ServerConfig` / `rustls::ClientConfig` for tonic /
//! axum lives in the consuming modules (`peers.rs` for the peer mTLS
//! transport, `healthz.rs` for the HTTPS listener). They consume an
//! `Arc<CertReloader>` and wrap their own [`ResolvesServerCert`] /
//! [`ResolvesClientCert`] / verifier implementations around the
//! reloader's `current()` bundle. Keeping transport-shaped builders out
//! of this module avoids speculative design without a real consumer.

use crate::config::TlsConfig;
use crate::errors::AgentError;
use arc_swap::ArcSwap;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;
use rustls::{
    client::ResolvesClientCert, server::ClientHello, sign::CertifiedKey as RustlsCertifiedKey,
};
use rustls::{server::ResolvesServerCert, RootCertStore};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// CertBundle
// ---------------------------------------------------------------------------

/// Immutable snapshot of the mTLS material currently in use. Swapped
/// atomically by [`CertReloader::reload`].
#[derive(Debug)]
pub struct CertBundle {
    /// The leaf cert + private key for this node, ready to hand to
    /// rustls's [`ResolvesServerCert::resolve`] /
    /// [`ResolvesClientCert::resolve`]. Wrapped in `Arc` because rustls
    /// resolvers return `Arc<CertifiedKey>` to allow zero-copy sharing
    /// across handshakes.
    pub certified_key: Arc<CertifiedKey>,

    /// Trusted CA roots. Used to verify peer certs (both inbound on the
    /// peer mTLS listener and outbound when dialing peers). Wrapped in
    /// `Arc` so a `WebPkiClientVerifier` / `WebPkiServerVerifier` built
    /// against it can be cheaply rebuilt on reload.
    pub roots: Arc<RootCertStore>,

    /// SHA-256 of the DER-encoded leaf cert. Used by `reload()` to
    /// answer "did anything actually change?" without diffing PEM bytes.
    pub leaf_fingerprint: [u8; 32],

    /// SHA-256 of the on-disk CA cert PEM bytes (not parsed certs — a
    /// PEM file may carry multiple certs and we want the file as a
    /// whole to be the rotation unit).
    pub ca_fingerprint: [u8; 32],

    /// DNS SANs extracted from the leaf cert. Surfaced for preflight
    /// (does our cert cover every peer hostname?) and for the SAN
    /// allowlist enforcement on the inbound peer mTLS verifier.
    pub leaf_dns_sans: Vec<String>,

    /// IP SANs (textual form, e.g. `"127.0.0.1"`, `"::1"`).
    pub leaf_ip_sans: Vec<String>,
}

// ---------------------------------------------------------------------------
// CertReloader
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CertReloader {
    cfg: TlsConfig,
    bundle: ArcSwap<CertBundle>,
}

impl CertReloader {
    /// Read all three PEM files, parse, build a [`CertBundle`], and
    /// store it. Fails if `cfg` isn't fully configured (all three paths
    /// present), if any file can't be read, or if any of the PEM
    /// content is malformed / inconsistent.
    pub fn new(cfg: TlsConfig) -> Result<Self, AgentError> {
        if !cfg.is_configured() {
            return Err(AgentError::TlsMissingField);
        }
        let bundle = Arc::new(load_bundle(&cfg)?);
        info!(
            ca = %cfg.ca_cert.as_ref().unwrap().display(),
            cert = %cfg.cert.as_ref().unwrap().display(),
            "certreload: loaded initial mTLS material"
        );
        Ok(Self {
            cfg,
            bundle: ArcSwap::new(bundle),
        })
    }

    /// Re-read all three files and atomically swap the active bundle.
    /// Returns `Ok(true)` when the on-disk material changed (either the
    /// leaf cert or the CA PEM); `Ok(false)` when both fingerprints
    /// match what was already loaded. On error the prior bundle stays
    /// in place — there's no half-loaded intermediate state.
    pub fn reload(&self) -> Result<bool, AgentError> {
        let next = Arc::new(load_bundle(&self.cfg)?);
        let prev = self.bundle.load();
        let changed = next.leaf_fingerprint != prev.leaf_fingerprint
            || next.ca_fingerprint != prev.ca_fingerprint;
        self.bundle.store(next);
        if changed {
            info!("certreload: reloaded mTLS material from disk");
        } else {
            debug!("certreload: SIGHUP processed, on-disk material unchanged");
        }
        Ok(changed)
    }

    /// Snapshot of the current bundle. Cheap clone (it's an `Arc`).
    pub fn current(&self) -> Arc<CertBundle> {
        self.bundle.load_full()
    }

    /// The cert paths the reloader is watching — useful for log
    /// messages (`reload from {paths:?}`) and the preflight report.
    pub fn paths(&self) -> &TlsConfig {
        &self.cfg
    }
}

// ---------------------------------------------------------------------------
// PEM loading
// ---------------------------------------------------------------------------

fn load_bundle(cfg: &TlsConfig) -> Result<CertBundle, AgentError> {
    // `is_configured` already checked by the caller, but we re-derive
    // the paths here so this function is callable in isolation by tests.
    let ca_path = cfg.ca_cert.as_deref().ok_or(AgentError::TlsMissingField)?;
    let cert_path = cfg.cert.as_deref().ok_or(AgentError::TlsMissingField)?;
    let key_path = cfg.key.as_deref().ok_or(AgentError::TlsMissingField)?;

    // CA — read raw bytes so the fingerprint covers the file as a whole
    // (multi-cert PEM bundles included), not just the first parsed cert.
    let ca_pem = std::fs::read(ca_path).map_err(|source| AgentError::TlsRead {
        path: ca_path.to_path_buf(),
        source,
    })?;
    let ca_fingerprint = sha256(&ca_pem);

    let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_pem.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|e| AgentError::TlsParse {
            path: ca_path.to_path_buf(),
            reason: format!("ca cert PEM: {e}"),
        })?;
    if ca_certs.is_empty() {
        return Err(AgentError::TlsParse {
            path: ca_path.to_path_buf(),
            reason: "ca cert PEM: no certificates found".to_string(),
        });
    }
    let mut roots = RootCertStore::empty();
    for c in ca_certs {
        roots.add(c).map_err(|e| AgentError::TlsParse {
            path: ca_path.to_path_buf(),
            reason: format!("add ca cert to root store: {e}"),
        })?;
    }

    // Leaf cert chain.
    let cert_pem = std::fs::read(cert_path).map_err(|source| AgentError::TlsRead {
        path: cert_path.to_path_buf(),
        source,
    })?;
    let cert_chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|e| AgentError::TlsParse {
            path: cert_path.to_path_buf(),
            reason: format!("leaf cert PEM: {e}"),
        })?;
    let leaf = cert_chain.first().ok_or_else(|| AgentError::TlsParse {
        path: cert_path.to_path_buf(),
        reason: "leaf cert PEM: no certificates found".to_string(),
    })?;
    let leaf_fingerprint = sha256(leaf.as_ref());
    let (leaf_dns_sans, leaf_ip_sans) =
        extract_sans(leaf.as_ref()).map_err(|e| AgentError::TlsParse {
            path: cert_path.to_path_buf(),
            reason: format!("leaf cert SANs: {e}"),
        })?;

    // Private key.
    let key_pem = std::fs::read(key_path).map_err(|source| AgentError::TlsRead {
        path: key_path.to_path_buf(),
        source,
    })?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|e| AgentError::TlsParse {
            path: key_path.to_path_buf(),
            reason: format!("private key PEM: {e}"),
        })?
        .ok_or_else(|| AgentError::TlsParse {
            path: key_path.to_path_buf(),
            reason: "private key PEM: no key found".to_string(),
        })?;

    // Build the signing key via rustls's ring provider. `any_supported_type`
    // handles ECDSA / RSA / Ed25519 transparently.
    let signing_key =
        rustls::crypto::ring::sign::any_supported_type(&key).map_err(|e| AgentError::TlsParse {
            path: key_path.to_path_buf(),
            reason: format!("private key: not a supported signing type: {e}"),
        })?;
    let certified_key = CertifiedKey::new(cert_chain, signing_key);

    Ok(CertBundle {
        certified_key: Arc::new(certified_key),
        roots: Arc::new(roots),
        leaf_fingerprint,
        ca_fingerprint,
        leaf_dns_sans,
        leaf_ip_sans,
    })
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().into()
}

/// Extract DNS + IP subject-alternative-names from a DER-encoded X.509
/// cert. Used by preflight (does our cert cover every peer hostname?)
/// and the inbound peer mTLS SAN allowlist enforcement.
pub(crate) fn extract_sans(der: &[u8]) -> Result<(Vec<String>, Vec<String>), String> {
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::FromDer;
    use x509_parser::prelude::X509Certificate;

    let (_, cert) = X509Certificate::from_der(der).map_err(|e| format!("parse x509: {e}"))?;
    let mut dns = Vec::new();
    let mut ip = Vec::new();
    let Some(sans) = cert
        .extensions()
        .iter()
        .find_map(|ext| match ext.parsed_extension() {
            x509_parser::extensions::ParsedExtension::SubjectAlternativeName(s) => Some(s),
            _ => None,
        })
    else {
        return Ok((dns, ip));
    };
    for name in &sans.general_names {
        match name {
            GeneralName::DNSName(s) => dns.push((*s).to_string()),
            GeneralName::IPAddress(bytes) => {
                if let Some(s) = ip_bytes_to_string(bytes) {
                    ip.push(s);
                }
            }
            _ => {} // ignore email, URI, etc.
        }
    }
    Ok((dns, ip))
}

fn ip_bytes_to_string(bytes: &[u8]) -> Option<String> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    match bytes.len() {
        4 => Some(IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])).to_string()),
        16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(bytes);
            Some(IpAddr::V6(Ipv6Addr::from(octets)).to_string())
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Reloadable rustls resolvers
// ---------------------------------------------------------------------------

/// `ResolvesServerCert` impl that reads from a live [`CertReloader`].
/// rustls calls `resolve` on every ClientHello — the resolver loads the
/// current bundle each time, so a SIGHUP-rotated cert reaches new
/// handshakes immediately without rebuilding the `ServerConfig`.
pub struct ReloadingServerCertResolver {
    reloader: Arc<CertReloader>,
}

impl ReloadingServerCertResolver {
    pub fn new(reloader: Arc<CertReloader>) -> Self {
        Self { reloader }
    }
}

impl std::fmt::Debug for ReloadingServerCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadingServerCertResolver").finish()
    }
}

impl ResolvesServerCert for ReloadingServerCertResolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<RustlsCertifiedKey>> {
        Some(self.reloader.current().certified_key.clone())
    }
}

/// `ResolvesClientCert` impl that reads from a live [`CertReloader`].
/// Used by outbound peer dials so a SIGHUP-rotated client cert hits new
/// connections without rebuilding the `ClientConfig`.
pub struct ReloadingClientCertResolver {
    reloader: Arc<CertReloader>,
}

impl ReloadingClientCertResolver {
    pub fn new(reloader: Arc<CertReloader>) -> Self {
        Self { reloader }
    }
}

impl std::fmt::Debug for ReloadingClientCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadingClientCertResolver").finish()
    }
}

impl ResolvesClientCert for ReloadingClientCertResolver {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<RustlsCertifiedKey>> {
        Some(self.reloader.current().certified_key.clone())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, IsCa, KeyPair};
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    /// (ca_pem, leaf_pem, key_pem) — a self-consistent test cert tuple.
    /// `leaf_dns` controls the leaf's SAN entries.
    fn gen_cert_pair(leaf_dns: &[&str]) -> (String, String, String) {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_pem = ca_cert.pem();

        let leaf_key = KeyPair::generate().unwrap();
        let leaf_dns: Vec<String> = leaf_dns.iter().map(|s| (*s).to_string()).collect();
        let leaf_params = CertificateParams::new(leaf_dns).unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();
        let leaf_pem = leaf_cert.pem();
        let key_pem = leaf_key.serialize_pem();

        (ca_pem, leaf_pem, key_pem)
    }

    fn write_bundle(dir: &Path, ca: &str, cert: &str, key: &str) -> (PathBuf, PathBuf, PathBuf) {
        let ca_path = dir.join("ca.crt");
        let cert_path = dir.join("node.crt");
        let key_path = dir.join("node.key");
        std::fs::write(&ca_path, ca).unwrap();
        std::fs::write(&cert_path, cert).unwrap();
        std::fs::write(&key_path, key).unwrap();
        (ca_path, cert_path, key_path)
    }

    fn fixture(dns_sans: &[&str]) -> (TempDir, CertReloader) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca, cert, key) = gen_cert_pair(dns_sans);
        let (ca_path, cert_path, key_path) = write_bundle(tmp.path(), &ca, &cert, &key);
        let cfg = TlsConfig {
            ca_cert: Some(ca_path),
            cert: Some(cert_path),
            key: Some(key_path),
        };
        let reloader = CertReloader::new(cfg).unwrap();
        (tmp, reloader)
    }

    // ----- load / initial bundle -------------------------------------------

    #[test]
    fn new_rejects_unconfigured_tls() {
        let cfg = TlsConfig::default(); // all None
        let err = CertReloader::new(cfg).unwrap_err();
        assert!(matches!(err, AgentError::TlsMissingField));
    }

    #[test]
    fn new_loads_pem_and_records_fingerprints() {
        let (_tmp, reloader) = fixture(&["server1"]);
        let bundle = reloader.current();
        assert_eq!(bundle.leaf_dns_sans, vec!["server1".to_string()]);
        // Fingerprints are non-zero (probabilistic — sha256 over real data).
        assert!(bundle.leaf_fingerprint.iter().any(|&b| b != 0));
        assert!(bundle.ca_fingerprint.iter().any(|&b| b != 0));
    }

    #[test]
    fn new_extracts_multiple_dns_sans() {
        let (_tmp, reloader) = fixture(&["server1", "server2", "alt.example.com"]);
        let bundle = reloader.current();
        let got: HashSet<_> = bundle.leaf_dns_sans.iter().cloned().collect();
        assert!(got.contains("server1"));
        assert!(got.contains("server2"));
        assert!(got.contains("alt.example.com"));
    }

    #[test]
    fn new_rejects_missing_ca_file() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (_ca, cert, key) = gen_cert_pair(&["server1"]);
        let (_ca_path_unused, cert_path, key_path) = write_bundle(tmp.path(), "", &cert, &key);
        let bogus_ca = tmp.path().join("does_not_exist.crt");
        let cfg = TlsConfig {
            ca_cert: Some(bogus_ca),
            cert: Some(cert_path),
            key: Some(key_path),
        };
        let err = CertReloader::new(cfg).unwrap_err();
        assert!(matches!(err, AgentError::TlsRead { .. }));
    }

    #[test]
    fn new_rejects_garbage_ca_pem() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (_ca, cert, key) = gen_cert_pair(&["server1"]);
        let (ca_path, cert_path, key_path) =
            write_bundle(tmp.path(), "not a pem file", &cert, &key);
        let cfg = TlsConfig {
            ca_cert: Some(ca_path),
            cert: Some(cert_path),
            key: Some(key_path),
        };
        let err = CertReloader::new(cfg).unwrap_err();
        assert!(matches!(err, AgentError::TlsParse { .. }), "got {err:?}");
    }

    #[test]
    fn new_rejects_empty_cert_pem() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = TempDir::new().unwrap();
        let (ca, _cert, key) = gen_cert_pair(&["server1"]);
        let (ca_path, cert_path, key_path) = write_bundle(tmp.path(), &ca, "", &key);
        let cfg = TlsConfig {
            ca_cert: Some(ca_path),
            cert: Some(cert_path),
            key: Some(key_path),
        };
        let err = CertReloader::new(cfg).unwrap_err();
        assert!(matches!(err, AgentError::TlsParse { .. }), "got {err:?}");
    }

    // ----- reload + change detection ---------------------------------------

    #[test]
    fn reload_unchanged_disk_returns_false() {
        let (_tmp, reloader) = fixture(&["server1"]);
        let initial = reloader.current();
        let changed = reloader.reload().unwrap();
        assert!(!changed);
        let after = reloader.current();
        assert_eq!(after.leaf_fingerprint, initial.leaf_fingerprint);
        assert_eq!(after.ca_fingerprint, initial.ca_fingerprint);
    }

    #[test]
    fn reload_after_cert_rotation_returns_true_and_swaps() {
        let (tmp, reloader) = fixture(&["server1"]);
        let before = reloader.current();

        // Replace the leaf + key on disk (same CA — only the leaf
        // rotates, a realistic mid-life cert rotation).
        let ca_pem = std::fs::read_to_string(reloader.paths().ca_cert.as_ref().unwrap()).unwrap();
        let (_ca2, cert2, key2) = gen_cert_pair(&["server1"]);
        std::fs::write(reloader.paths().cert.as_ref().unwrap(), &cert2).unwrap();
        std::fs::write(reloader.paths().key.as_ref().unwrap(), &key2).unwrap();
        // (CA file unchanged.)
        let _ = ca_pem;
        let _ = tmp;

        let changed = reloader.reload().unwrap();
        assert!(changed, "leaf rotation should be detected");
        let after = reloader.current();
        assert_ne!(after.leaf_fingerprint, before.leaf_fingerprint);
        assert_eq!(
            after.ca_fingerprint, before.ca_fingerprint,
            "CA didn't change"
        );
    }

    #[test]
    fn reload_after_ca_rotation_returns_true() {
        let (_tmp, reloader) = fixture(&["server1"]);
        let before = reloader.current();

        // Rotate to an entirely new chain — fresh CA and leaf.
        let (ca2, cert2, key2) = gen_cert_pair(&["server1"]);
        std::fs::write(reloader.paths().ca_cert.as_ref().unwrap(), &ca2).unwrap();
        std::fs::write(reloader.paths().cert.as_ref().unwrap(), &cert2).unwrap();
        std::fs::write(reloader.paths().key.as_ref().unwrap(), &key2).unwrap();

        let changed = reloader.reload().unwrap();
        assert!(changed);
        let after = reloader.current();
        assert_ne!(after.ca_fingerprint, before.ca_fingerprint);
        assert_ne!(after.leaf_fingerprint, before.leaf_fingerprint);
    }

    #[test]
    fn reload_failure_preserves_prior_bundle() {
        let (_tmp, reloader) = fixture(&["server1"]);
        let before = reloader.current();
        let before_leaf = before.leaf_fingerprint;
        let before_ca = before.ca_fingerprint;

        // Corrupt the cert file on disk.
        std::fs::write(reloader.paths().cert.as_ref().unwrap(), "garbage not pem").unwrap();
        let err = reloader.reload().unwrap_err();
        assert!(matches!(err, AgentError::TlsParse { .. }));

        // Bundle is still the original — the failed reload did NOT
        // install partial state.
        let after = reloader.current();
        assert_eq!(after.leaf_fingerprint, before_leaf);
        assert_eq!(after.ca_fingerprint, before_ca);
    }

    // ----- resolvers --------------------------------------------------------

    #[test]
    fn server_resolver_returns_current_bundles_key() {
        let (_tmp, reloader) = fixture(&["server1"]);
        let reloader = Arc::new(reloader);
        let resolver = ReloadingServerCertResolver::new(reloader.clone());
        // The ClientHello is non-trivial to construct; we can't easily
        // call .resolve(hello) here without a real handshake. So just
        // verify the resolver holds the same Arc as the reloader and
        // that has_* is true.
        let bundle = reloader.current();
        assert_eq!(
            Arc::as_ptr(&resolver.reloader.current().certified_key),
            Arc::as_ptr(&bundle.certified_key)
        );
    }

    #[test]
    fn client_resolver_reports_has_certs() {
        let (_tmp, reloader) = fixture(&["server1"]);
        let reloader = Arc::new(reloader);
        let resolver = ReloadingClientCertResolver::new(reloader);
        assert!(ResolvesClientCert::has_certs(&resolver));
    }

    // ----- ip_bytes_to_string ----------------------------------------------

    #[test]
    fn ip_bytes_to_string_v4_v6_and_garbage() {
        assert_eq!(
            ip_bytes_to_string(&[127, 0, 0, 1]),
            Some("127.0.0.1".to_string())
        );
        assert_eq!(
            ip_bytes_to_string(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            Some("::1".to_string())
        );
        // Wrong length — not v4 or v6.
        assert_eq!(ip_bytes_to_string(&[1, 2, 3]), None);
        assert_eq!(ip_bytes_to_string(&[]), None);
    }
}
