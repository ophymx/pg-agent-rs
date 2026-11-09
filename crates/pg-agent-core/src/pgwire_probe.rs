//! Two questions asked on the PostgreSQL wire without libpq and without
//! a credential:
//!
//! 1. **Would this server admit a connection from here, as this user,
//!    to this database?** — the first thing a PostgreSQL server does
//!    with a startup message is match it against `pg_hba.conf`, and it
//!    answers before any password changes hands: either an
//!    authentication request (the rule matched) or an `ErrorResponse`
//!    (`no pg_hba.conf entry for host …`). So the check that pgpool's
//!    health check and `sr_check` will pass can be made with exactly
//!    the bytes they send and none of the secrets they hold.
//!
//! 2. **Which certificate is this server serving right now?** — the
//!    SSLRequest handshake, with a verifier that accepts anything and
//!    keeps the chain. The agent compares it with the file on disk to
//!    tell "renewed" from "loaded" (`pgpool_tls`).
//!
//! Both deliberately follow libpq's shape: SSLRequest first and fall
//! back to plaintext on `N`, which is `sslmode=prefer` — pgpool's
//! health check uses libpq defaults, so a `hostssl`-only `pg_hba.conf`
//! is judged the way pgpool would experience it.

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// SSLRequest code (protocol 1234.5679).
const SSL_REQUEST_CODE: u32 = 80877103;
/// Protocol 3.0 startup code.
const PROTOCOL_V3: u32 = 196608;
/// Upper bound on a response we are willing to read — an ErrorResponse
/// or an authentication request is a few hundred bytes.
const MAX_MESSAGE: usize = 64 * 1024;

/// Outcome of [`probe_admission`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// `pg_hba.conf` matched: the server asked for credentials (or
    /// none, under `trust`). `auth` names the method it asked for.
    Admitted { tls: bool, auth: String },
    /// The server refused before authentication. `message` is the
    /// server's own text — for a `pg_hba.conf` miss it names the
    /// address, user, database and whether SSL was on, which is the
    /// whole diagnosis.
    Rejected {
        tls: bool,
        sqlstate: String,
        message: String,
    },
    /// No PostgreSQL answered on the wire: connect failed, timed out,
    /// or something that is not PostgreSQL replied.
    Unreachable { error: String },
}

/// Ask `addr` whether it admits `user`@`database` from here. `sni` is
/// the hostname the connection is for (used for TLS SNI only).
pub async fn probe_admission(
    addr: SocketAddr,
    sni: &str,
    user: &str,
    database: &str,
    timeout: Duration,
) -> Admission {
    match tokio::time::timeout(timeout, probe_admission_inner(addr, sni, user, database)).await {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => Admission::Unreachable {
            error: e.to_string(),
        },
        Err(_) => Admission::Unreachable {
            error: format!("no answer within {}s", timeout.as_secs()),
        },
    }
}

async fn probe_admission_inner(
    addr: SocketAddr,
    sni: &str,
    user: &str,
    database: &str,
) -> anyhow::Result<Admission> {
    let mut tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| anyhow::anyhow!("connect {addr}: {e}"))?;
    tcp.write_all(&ssl_request()).await?;
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await?;
    match answer[0] {
        b'S' => {
            let mut tls = tls_connect(tcp, sni).await?;
            let outcome = startup_exchange(&mut tls, user, database, true).await;
            let _ = tls.shutdown().await;
            outcome
        }
        b'N' => {
            let outcome = startup_exchange(&mut tcp, user, database, false).await;
            let _ = tcp.shutdown().await;
            outcome
        }
        other => {
            anyhow::bail!("{addr}: not PostgreSQL (SSLRequest answered with byte {other:#04x})")
        }
    }
}

async fn startup_exchange<S>(
    stream: &mut S,
    user: &str,
    database: &str,
    tls: bool,
) -> anyhow::Result<Admission>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    stream.write_all(&startup_message(user, database)).await?;
    let (kind, body) = read_message(stream).await?;
    Ok(match kind {
        b'R' => {
            let code = body
                .get(..4)
                .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                .unwrap_or(u32::MAX);
            Admission::Admitted {
                tls,
                auth: auth_method_name(code),
            }
        }
        b'E' => {
            let (sqlstate, message) = parse_error_fields(&body);
            Admission::Rejected {
                tls,
                sqlstate,
                message,
            }
        }
        other => anyhow::bail!("unexpected startup reply {other:#04x}"),
    })
}

/// The certificate chain `addr` serves after an SSLRequest, leaf first,
/// DER-encoded. `Err` when the server declined SSL or the handshake
/// failed.
pub async fn fetch_served_certificate(
    addr: SocketAddr,
    sni: &str,
    timeout: Duration,
) -> anyhow::Result<Vec<Vec<u8>>> {
    tokio::time::timeout(timeout, async {
        let mut tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| anyhow::anyhow!("connect {addr}: {e}"))?;
        tcp.write_all(&ssl_request()).await?;
        let mut answer = [0u8; 1];
        tcp.read_exact(&mut answer).await?;
        if answer[0] != b'S' {
            anyhow::bail!(
                "{addr}: server declined SSL (answered {:?})",
                answer[0] as char
            );
        }
        let tls = tls_connect(tcp, sni).await?;
        let chain: Vec<Vec<u8>> = tls
            .get_ref()
            .1
            .peer_certificates()
            .map(|certs| certs.iter().map(|c| c.as_ref().to_vec()).collect())
            .unwrap_or_default();
        if chain.is_empty() {
            anyhow::bail!("{addr}: handshake completed without a server certificate");
        }
        Ok(chain)
    })
    .await
    .map_err(|_| anyhow::anyhow!("{addr}: no TLS answer within {}s", timeout.as_secs()))?
}

async fn tls_connect(
    tcp: TcpStream,
    sni: &str,
) -> anyhow::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        .with_no_client_auth();
    let name = ServerName::try_from(sni.to_string())
        .map_err(|e| anyhow::anyhow!("server name {sni:?}: {e}"))?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    Ok(connector.connect(name, tcp).await?)
}

/// Verifier that accepts every certificate. This module OBSERVES what
/// a server presents; it never relies on the connection for anything
/// secret, and the one thing it sends (a startup message naming a
/// user) is what any client on the network could send.
#[derive(Debug)]
struct AcceptAny(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// Wire helpers (pure, tested)
// ---------------------------------------------------------------------------

fn ssl_request() -> [u8; 8] {
    let mut b = [0u8; 8];
    b[..4].copy_from_slice(&8u32.to_be_bytes());
    b[4..].copy_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
    b
}

fn startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    for (k, v) in [("user", user), ("database", database)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = Vec::with_capacity(body.len() + 4);
    msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    msg.extend_from_slice(&body);
    msg
}

async fn read_message<S>(stream: &mut S) -> anyhow::Result<(u8, Vec<u8>)>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut head = [0u8; 5];
    stream.read_exact(&mut head).await?;
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    if !(4..=MAX_MESSAGE).contains(&len) {
        anyhow::bail!("message length {len} out of range");
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await?;
    Ok((head[0], body))
}

/// `ErrorResponse` body → (SQLSTATE, message). Fields are `type byte +
/// NUL-terminated string`, terminated by a zero byte.
fn parse_error_fields(body: &[u8]) -> (String, String) {
    let mut sqlstate = String::new();
    let mut message = String::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let kind = body[i];
        let end = body[i + 1..]
            .iter()
            .position(|b| *b == 0)
            .map(|p| i + 1 + p)
            .unwrap_or(body.len());
        let text = String::from_utf8_lossy(&body[i + 1..end]).into_owned();
        match kind {
            b'C' => sqlstate = text,
            b'M' => message = text,
            _ => {}
        }
        i = end + 1;
    }
    (sqlstate, message)
}

fn auth_method_name(code: u32) -> String {
    match code {
        0 => "trust".into(),
        3 => "password".into(),
        5 => "md5".into(),
        7 => "gss".into(),
        9 => "sspi".into(),
        10 => "scram-sha-256".into(),
        other => format!("auth request {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn startup_message_is_what_libpq_sends() {
        let m = startup_message("pgpool", "postgres");
        assert_eq!(&m[..4], &(m.len() as u32).to_be_bytes());
        assert_eq!(&m[4..8], &PROTOCOL_V3.to_be_bytes());
        assert_eq!(&m[8..], b"user\0pgpool\0database\0postgres\0\0");
    }

    #[test]
    fn error_fields_extract_code_and_message() {
        let body = b"SFATAL\0C28000\0Mno pg_hba.conf entry for host \"fd00::1\", user \"pgpool\", database \"postgres\", SSL encryption\0Fauth.c\0\0";
        let (code, msg) = parse_error_fields(body);
        assert_eq!(code, "28000");
        assert!(msg.starts_with("no pg_hba.conf entry"));
    }

    /// A fake server that answers the SSLRequest with `N` and the
    /// startup with the given message.
    async fn fake_server(reply: Vec<u8>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut ssl = [0u8; 8];
            s.read_exact(&mut ssl).await.unwrap();
            s.write_all(b"N").await.unwrap();
            let mut head = [0u8; 4];
            s.read_exact(&mut head).await.unwrap();
            let len = u32::from_be_bytes(head) as usize;
            let mut rest = vec![0u8; len - 4];
            s.read_exact(&mut rest).await.unwrap();
            s.write_all(&reply).await.unwrap();
        });
        addr
    }

    fn framed(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut m = vec![kind];
        m.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        m.extend_from_slice(body);
        m
    }

    #[tokio::test]
    async fn admitted_when_server_asks_for_a_password() {
        let addr = fake_server(framed(b'R', &10u32.to_be_bytes())).await;
        let a = probe_admission(
            addr,
            "localhost",
            "pgpool",
            "postgres",
            Duration::from_secs(2),
        )
        .await;
        assert_eq!(
            a,
            Admission::Admitted {
                tls: false,
                auth: "scram-sha-256".into()
            }
        );
    }

    #[tokio::test]
    async fn rejected_carries_the_servers_own_words() {
        let body = b"SFATAL\0C28000\0Mno pg_hba.conf entry for host \"fd00::1\", user \"pgpool\", database \"postgres\", no encryption\0\0";
        let addr = fake_server(framed(b'E', body)).await;
        let a = probe_admission(
            addr,
            "localhost",
            "pgpool",
            "postgres",
            Duration::from_secs(2),
        )
        .await;
        match a {
            Admission::Rejected {
                sqlstate, message, ..
            } => {
                assert_eq!(sqlstate, "28000");
                assert!(message.contains("fd00::1"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn nothing_listening_is_unreachable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let a = probe_admission(
            addr,
            "localhost",
            "pgpool",
            "postgres",
            Duration::from_secs(2),
        )
        .await;
        assert!(matches!(a, Admission::Unreachable { .. }), "{a:?}");
    }

    #[tokio::test]
    async fn a_server_that_is_not_postgres_is_unreachable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            s.write_all(b"HTTP/1.0 400 Bad Request\r\n\r\n")
                .await
                .unwrap();
        });
        let a = probe_admission(
            addr,
            "localhost",
            "pgpool",
            "postgres",
            Duration::from_secs(2),
        )
        .await;
        match a {
            Admission::Unreachable { error } => {
                assert!(error.contains("not PostgreSQL"), "{error}")
            }
            other => panic!("{other:?}"),
        }
    }
}
