//! `RedisTlsConfig` (`TlsConfigFromFiles` with a server CA): loaded with the settings when
//! either pool uses TLS (`REDIS_TLS`, `REDIS_PERSECOND_TLS`). The server is verified against the system's roots plus `REDIS_TLS_CACERT`; `REDIS_TLS_CLIENT_CERT` and
//! `REDIS_TLS_CLIENT_KEY` are the client's certificate; with
//! `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`, the chain is verified but not the name.

use crate::settings::Settings;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use std::sync::Arc;

/// The client configuration both pools share, if either uses TLS; an error is what upstream
/// panics with.
pub fn from_settings(s: &Settings) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
    if !(s.redis_tls || s.redis_persecond_tls) {
        return Ok(None);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    // A key pair needs both files.
    let identity = match (
        s.redis_tls_client_cert.as_str(),
        s.redis_tls_client_key.as_str(),
    ) {
        ("", _) | (_, "") => None,
        (cert, key) => Some(
            key_pair(cert, key, &provider)
                .map_err(|e| format!("failed lo load TLS key pair ({cert},{key}): {e}"))?,
        ),
    };
    let ca = s.redis_tls_cacert.as_str();
    if !ca.is_empty() {
        let pem = read(ca).map_err(|e| format!("failed to read file: {ca}: {e}"))?;
        let certs: Vec<_> = CertificateDer::pem_slice_iter(&pem)
            .filter_map(Result::ok)
            .collect();
        let added = certs
            .into_iter()
            .filter(|c| roots.add(c.clone()).is_ok())
            .count();
        if added == 0 {
            return Err(format!(
                "failed to load the provided TLS CA certificate: {ca}"
            ));
        }
    }
    let roots = Arc::new(roots);
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
    let builder = match s.redis_tls_skip_hostname_verification {
        true => {
            // With no root at all, every handshake fails, as Go's verification would.
            let verifier =
                rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
                    .build()
                    .ok();
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NameNotVerified(verifier)))
        }
        false => builder.with_root_certificates(roots),
    };
    let config = match identity {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("failed lo load TLS key pair: {e}"))?,
        None => builder.with_no_client_auth(),
    };
    Ok(Some(Arc::new(config)))
}

/// `os.ReadFile`'s error as Go writes it.
fn read(path: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| {
        let op = match e.kind() {
            std::io::ErrorKind::IsADirectory => "read",
            _ => "open",
        };
        format!("{op} {path}: {}", crate::go::errno_text(&e))
    })
}

/// `tls.LoadX509KeyPair`: the certificate file, then the key file, then the pair.
fn key_pair(
    cert: &str,
    key: &str,
    provider: &rustls::crypto::CryptoProvider,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let cert_pem = read(cert)?;
    let key_pem = read(key)?;
    let certs: Vec<_> = CertificateDer::pem_slice_iter(&cert_pem)
        .filter_map(Result::ok)
        .collect();
    if certs.is_empty() {
        return Err("tls: failed to find any PEM data in certificate input".into());
    }
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|_| "tls: failed to find any PEM data in key input".to_string())?;
    let signing = provider
        .key_provider
        .load_private_key(key.clone_key())
        .map_err(|e| format!("tls: {e}"))?;
    rustls::sign::CertifiedKey::new(certs.clone(), signing)
        .keys_match()
        .map_err(|_| "tls: private key does not match public key".to_string())?;
    Ok((certs, key))
}

/// The chain verified against the roots, the name not (Go's `InsecureSkipVerify` with its
/// own `VerifyPeerCertificate`).
#[derive(Debug)]
struct NameNotVerified(Option<Arc<rustls::client::WebPkiServerVerifier>>);

impl NameNotVerified {
    fn inner(&self) -> Result<&rustls::client::WebPkiServerVerifier, rustls::Error> {
        self.0.as_deref().ok_or(rustls::Error::InvalidCertificate(
            rustls::CertificateError::UnknownIssuer,
        ))
    }
}

impl rustls::client::danger::ServerCertVerifier for NameNotVerified {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error};
        match self.inner()?.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(rustls::client::danger::ServerCertVerified::assertion()),
            verified => verified,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner()?.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner()?.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        match &self.0 {
            Some(verifier) => verifier.supported_verify_schemes(),
            None => rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}
