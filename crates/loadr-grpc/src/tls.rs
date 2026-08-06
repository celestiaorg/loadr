//! Shared rustls client configuration built once per handler from
//! [`loadr_config::TlsConfig`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use loadr_config::TlsConfig;
use loadr_core::error::ProtocolError;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

/// Resolve a possibly-relative path against the test definition directory.
pub(crate) fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

/// Certificate verifier that accepts everything (`insecure_skip_verify`).
#[derive(Debug)]
struct NoVerify {
    schemes: Vec<SignatureScheme>,
}

impl NoVerify {
    fn new() -> Self {
        NoVerify {
            schemes: rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl ServerCertVerifier for NoVerify {
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
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

fn read_pem_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, ProtocolError> {
    let data = std::fs::read(path)
        .map_err(|e| ProtocolError::Tls(format!("cannot read PEM file {}: {e}", path.display())))?;
    let mut certs = Vec::new();
    for cert in rustls_pemfile::certs(&mut data.as_slice()) {
        let cert = cert.map_err(|e| {
            ProtocolError::Tls(format!("invalid certificate in {}: {e}", path.display()))
        })?;
        certs.push(cert);
    }
    if certs.is_empty() {
        return Err(ProtocolError::Tls(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn read_pem_key(path: &Path) -> Result<PrivateKeyDer<'static>, ProtocolError> {
    let data = std::fs::read(path)
        .map_err(|e| ProtocolError::Tls(format!("cannot read key file {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut data.as_slice())
        .map_err(|e| ProtocolError::Tls(format!("invalid key file {}: {e}", path.display())))?
        .ok_or_else(|| ProtocolError::Tls(format!("no private key found in {}", path.display())))
}

/// Build a rustls [`rustls::ClientConfig`] from loadr TLS settings.
///
/// Roots default to the bundled webpki roots; `ca_file` adds extra roots,
/// `cert_file`/`key_file` enable client auth and `insecure_skip_verify`
/// installs a verifier that accepts any certificate.
pub(crate) fn client_config(
    tls: &TlsConfig,
    base_dir: &Path,
    alpn: Vec<Vec<u8>>,
) -> Result<rustls::ClientConfig, ProtocolError> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(ca) = &tls.ca_file {
        let path = resolve_path(base_dir, ca);
        for cert in read_pem_certs(&path)? {
            roots.add(cert).map_err(|e| {
                ProtocolError::Tls(format!("cannot add CA from {}: {e}", path.display()))
            })?;
        }
    }

    // Restrict TLS versions when configured (default: 1.2 + 1.3).
    let parse_ver = |v: &str| -> Result<u8, ProtocolError> {
        match v
            .trim()
            .trim_start_matches("1.")
            .trim_start_matches("TLSv1.")
        {
            "2" => Ok(2),
            "3" => Ok(3),
            other => Err(ProtocolError::Tls(format!(
                "unsupported TLS version `{other}` (use `1.2` or `1.3`)"
            ))),
        }
    };
    let min = tls.min_version.as_deref().map(parse_ver).transpose()?;
    let max = tls.max_version.as_deref().map(parse_ver).transpose()?;
    let mut versions: Vec<&'static rustls::SupportedProtocolVersion> = Vec::new();
    for (n, v) in [
        (2u8, &rustls::version::TLS12),
        (3u8, &rustls::version::TLS13),
    ] {
        if min.map(|m| n >= m).unwrap_or(true) && max.map(|m| n <= m).unwrap_or(true) {
            versions.push(v);
        }
    }
    if versions.is_empty() {
        return Err(ProtocolError::Tls(
            "TLS min_version/max_version exclude every supported version".to_string(),
        ));
    }
    let builder = if tls.min_version.is_some() || tls.max_version.is_some() {
        rustls::ClientConfig::builder_with_protocol_versions(&versions)
            .with_root_certificates(roots)
    } else {
        rustls::ClientConfig::builder().with_root_certificates(roots)
    };
    let mut config = match (&tls.cert_file, &tls.key_file) {
        (Some(cert), Some(key)) => {
            let certs = read_pem_certs(&resolve_path(base_dir, cert))?;
            let key = read_pem_key(&resolve_path(base_dir, key))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| ProtocolError::Tls(format!("invalid client certificate: {e}")))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(ProtocolError::Tls(
                "tls `cert_file` and `key_file` must be configured together".to_string(),
            ))
        }
    };

    if tls.insecure_skip_verify {
        tracing::warn!("TLS certificate verification is disabled (insecure_skip_verify)");
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerify::new()));
    }
    config.alpn_protocols = alpn;
    Ok(config)
}

/// SNI server name for `url`, honoring the `server_name` override.
pub(crate) fn server_name(
    override_name: Option<&str>,
    url: &url::Url,
) -> Result<ServerName<'static>, ProtocolError> {
    if let Some(name) = override_name {
        return ServerName::try_from(name.to_string())
            .map_err(|e| ProtocolError::Tls(format!("invalid tls server_name `{name}`: {e}")));
    }
    match url.host() {
        Some(url::Host::Domain(d)) => ServerName::try_from(d.to_string())
            .map_err(|e| ProtocolError::Tls(format!("invalid server name `{d}`: {e}"))),
        Some(url::Host::Ipv4(ip)) => Ok(ServerName::IpAddress(ip.into())),
        Some(url::Host::Ipv6(ip)) => Ok(ServerName::IpAddress(ip.into())),
        None => Err(ProtocolError::InvalidRequest(format!(
            "url `{url}` has no host"
        ))),
    }
}
