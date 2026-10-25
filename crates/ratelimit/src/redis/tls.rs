//! `RedisTlsConfig` (`TlsConfigFromFiles` with a server CA): loaded with the settings when
//! either pool uses TLS (`REDIS_TLS`, `REDIS_PERSECOND_TLS`). The server is verified against the system's roots plus `REDIS_TLS_CACERT`; `REDIS_TLS_CLIENT_CERT` and
//! `REDIS_TLS_CLIENT_KEY` are the client's certificate; with
//! `REDIS_TLS_SKIP_HOSTNAME_VERIFICATION`, the chain is verified but not the name.

use crate::settings::Settings;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use std::sync::Arc;

/// The client configuration both pools share, if either uses TLS; an error is what upstream
/// panics with.
pub fn from_settings(s: &Settings) -> Result<Option<Arc<rustls::ClientConfig>>, String> {
    if !(s.redis_tls || s.redis_persecond_tls) {
        return Ok(None);
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    // A key pair needs both files.
    let identity = match (
        s.redis_tls_client_cert.as_str(),
        s.redis_tls_client_key.as_str(),
    ) {
        ("", _) | (_, "") => None,
        (cert, key) => Some(
            crate::tls::key_pair(cert, key, &provider)
                .map_err(|e| format!("failed lo load TLS key pair ({cert},{key}): {e}"))?,
        ),
    };
    let roots = crate::tls::roots(&s.redis_tls_cacert)?;
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
        false => {
            let verifier =
                rustls::client::WebPkiServerVerifier::builder_with_provider(roots, provider)
                    .build()
                    .ok();
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(GoNames(verifier)))
        }
    };
    let config = match identity {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| format!("failed lo load TLS key pair: {e}"))?,
        None => builder.with_no_client_auth(),
    };
    Ok(Some(Arc::new(config)))
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

/// The chain verified against the roots, then the name as `Certificate.VerifyHostname`
/// matches it, a mismatch carrying `HostnameError`'s text.
#[derive(Debug)]
struct GoNames(Option<Arc<rustls::client::WebPkiServerVerifier>>);

/// `HostnameError`'s text, carried in a rustls error.
#[derive(Debug)]
struct HostnameError(String);

impl std::fmt::Display for HostnameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HostnameError {}

impl rustls::client::danger::ServerCertVerifier for GoNames {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use rustls::{CertificateError, Error};
        let verifier = NameNotVerified(self.0.clone());
        let verified = verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        let names = crate::tls::Names::of(end_entity)
            .ok_or(Error::InvalidCertificate(CertificateError::BadEncoding))?;
        match names.verify_hostname(&server_name.to_str()) {
            Ok(()) => Ok(verified),
            Err(e) => Err(Error::InvalidCertificate(CertificateError::Other(
                rustls::OtherError(Arc::new(HostnameError(e))),
            ))),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        NameNotVerified(self.0.clone()).verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        NameNotVerified(self.0.clone()).verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        NameNotVerified(self.0.clone()).supported_verify_schemes()
    }
}
