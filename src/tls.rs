//! TLS configuration for both ends, using rustls with the ring provider.
//!
//! The client trusts only the configured CA file; built-in roots are not loaded and
//! certificate verification is never disabled. SHA-256 checks content integrity; TLS and the
//! bearer token establish the communication and authorization boundaries (§9).

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let certificates: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("read certificates from {}: {e}", path.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("parse certificates in {}: {e}", path.display()))?;
    if certificates.is_empty() {
        return Err(format!("no certificates found in {}", path.display()));
    }
    Ok(certificates)
}

/// Server configuration from a PEM certificate chain and private key.
pub fn server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String> {
    let chain = load_certificates(cert_path)?;
    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| format!("read private key from {}: {e}", key_path.display()))?;
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| format!("TLS certificate: {e}"))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Client configuration trusting only the CA certificates in `ca_path`.
pub fn client_config(ca_path: &Path) -> Result<ClientConfig, String> {
    let mut roots = RootCertStore::empty();
    for certificate in load_certificates(ca_path)? {
        roots
            .add(certificate)
            .map_err(|e| format!("CA certificate in {}: {e}", ca_path.display()))?;
    }
    let mut config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS protocol versions: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// An HTTPS-only reqwest client that trusts only `ca_path`.
pub fn http_client(
    ca_path: &Path,
    connect_timeout: std::time::Duration,
    request_timeout: std::time::Duration,
) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .tls_backend_preconfigured(client_config(ca_path)?)
        .https_only(true)
        .http1_only()
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
        .map_err(|e| format!("HTTP client: {e}"))
}
