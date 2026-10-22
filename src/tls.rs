//! HTTPS, as chttpd's https listener: with `[ssl] enable = true`, the
//! same API on `[ssl] port` (6984), using cert_file and key_file (PEM).
//! With verify_ssl_certificates, clients' certificates are checked
//! against cacert_file, and fail_if_no_peer_cert requires one.
//! tls_versions (e.g. `[tlsv1.2, tlsv1.3]`) limits the versions offered.

use crate::config::Config;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// The versions `tls_versions` names (all when unset or unknown).
fn versions(config: &Config) -> Vec<&'static rustls::SupportedProtocolVersion> {
    let spec = config.get("ssl", "tls_versions").unwrap_or_default().to_ascii_lowercase();
    let mut v = Vec::new();
    if spec.contains("tlsv1.2") {
        v.push(&rustls::version::TLS12);
    }
    if spec.contains("tlsv1.3") {
        v.push(&rustls::version::TLS13);
    }
    if v.is_empty() { rustls::ALL_VERSIONS.to_vec() } else { v }
}

/// The TLS server setup [ssl] describes.
pub fn server_config(config: &Config) -> Result<Arc<rustls::ServerConfig>, String> {
    let (Some(cert_file), Some(key_file)) = (config.get("ssl", "cert_file"), config.get("ssl", "key_file")) else {
        return Err("SSL enabled but PEM certificates are missing.".into());
    };
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_file)
        .map_err(|e| format!("{cert_file}: {e}"))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("{cert_file}: {e}"))?;
    if config.get("ssl", "password").is_some() {
        tracing::warn!("[ssl] password is not supported: key_file must be an unencrypted PEM key");
    }
    let key = PrivateKeyDer::from_pem_file(&key_file).map_err(|e| format!("{key_file}: {e}"))?;
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&versions(config))
        .map_err(|e| e.to_string())?;
    let builder = if config.get_bool("ssl", "verify_ssl_certificates", false) {
        let ca = config.get("ssl", "cacert_file").ok_or("verify_ssl_certificates needs [ssl] cacert_file")?;
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_file_iter(&ca).map_err(|e| format!("{ca}: {e}"))? {
            roots.add(c.map_err(|e| format!("{ca}: {e}"))?).map_err(|e| format!("{ca}: {e}"))?;
        }
        let mut v = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider());
        if !config.get_bool("ssl", "fail_if_no_peer_cert", false) {
            v = v.allow_unauthenticated();
        }
        builder.with_client_cert_verifier(v.build().map_err(|e| e.to_string())?)
    } else {
        builder.with_no_client_auth()
    };
    let mut cfg = builder.with_single_cert(certs, key).map_err(|e| e.to_string())?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// Accepted connections, handshakes done (each in its own task, so a slow
/// client holds up no one).
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, std::net::SocketAddr)>,
    local: std::net::SocketAddr,
}

pub async fn listen(bind: &str, cfg: Arc<rustls::ServerConfig>) -> std::io::Result<TlsListener> {
    let tcp = TcpListener::bind(bind).await?;
    let local = tcp.local_addr()?;
    let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        loop {
            let (s, addr) = match tcp.accept().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("accept error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let (acceptor, tx) = (acceptor.clone(), tx.clone());
            tokio::spawn(async move {
                match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(s)).await {
                    Ok(Ok(tls)) => {
                        let _ = tx.send((tls, addr)).await;
                    }
                    Ok(Err(e)) => tracing::debug!("TLS handshake with {addr} failed: {e}"),
                    Err(_) => tracing::debug!("TLS handshake with {addr} timed out"),
                }
            });
        }
    });
    Ok(TlsListener { rx, local })
}

impl axum::serve::Listener for TlsListener {
    type Io = crate::system::Counted<TlsStream<TcpStream>>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if let Some((s, a)) = self.rx.recv().await {
                return (crate::system::Counted::new(s), a);
            }
            // The accept loop ended; nothing more will come.
            std::future::pending::<()>().await;
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}
