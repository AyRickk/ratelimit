//! The OTLP exporter (`otlptracegrpc`, `otlptracehttp`) behind the SDK's batch span
//! processor: spans queue up (2048 at most, `OTEL_BSP_MAX_QUEUE_SIZE`) and leave in batches
//! of 512 (`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`) every 5 s (`OTEL_BSP_SCHEDULE_DELAY`), to the
//! collector the `OTEL_EXPORTER_OTLP_*` variables configure.

use super::{Kind, Scope, SpanContext, Value};
use ratelimit_proto::opentelemetry::proto::{
    collector::trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::SpanKind, status::StatusCode},
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// The exporter's protocol (`TRACING_EXPORTER_PROTOCOL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Grpc,
    Http,
}

/// A span ended, waiting to be exported.
#[derive(Debug, Clone)]
pub(super) struct SpanData {
    pub scope: Scope,
    pub context: SpanContext,
    pub parent_span_id: Option<[u8; 8]>,
    pub parent_remote: bool,
    pub name: String,
    pub kind: Kind,
    pub start: u64,
    pub end: u64,
    pub attributes: Vec<(String, Value)>,
    /// `Error` with this message; unset otherwise.
    pub status: Option<String>,
}

/// The semantic conventions' schema of the resource (v1.7.0).
const RESOURCE_SCHEMA: &str = "https://opentelemetry.io/schemas/1.7.0";

/// `resource.NewWithAttributes` with the three service attributes, merged over
/// `resource.Environment()` (`OTEL_RESOURCE_ATTRIBUTES`, whose values are percent-decoded and
/// whose last value of a key wins, then `OTEL_SERVICE_NAME`): sorted by key, the service
/// attributes winning.
pub(super) fn resource(name: &str, namespace: &str, instance_id: &str) -> Resource {
    let mut attributes = std::collections::BTreeMap::new();
    let pairs = std::env::var("OTEL_RESOURCE_ATTRIBUTES").unwrap_or_default();
    let pairs = pairs.trim();
    let mut invalid = Vec::new();
    if !pairs.is_empty() {
        for pair in pairs.split(',') {
            let Some((key, value)) = pair.split_once('=') else {
                invalid.push(pair);
                continue;
            };
            let value =
                crate::go::unescape(value.trim(), false).unwrap_or_else(|| value.to_string());
            attributes.insert(key.trim().to_string(), value);
        }
    }
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_default();
    let service_name = service_name.trim();
    if !service_name.is_empty() {
        attributes.insert("service.name".to_string(), service_name.to_string());
    }
    if !invalid.is_empty() {
        otel_error(&format!(
            "partial resource: missing value: [{}]",
            invalid.join(" ")
        ));
    }
    for (key, value) in [
        ("service.name", name),
        ("service.namespace", namespace),
        ("service.instance.id", instance_id),
    ] {
        attributes.insert(key.to_string(), value.to_string());
    }
    Resource {
        attributes: attributes
            .into_iter()
            .map(|(k, v)| key_value(k, Value::Str(v)))
            .collect(),
        ..Default::default()
    }
}

fn key_value(key: String, value: Value) -> KeyValue {
    let value = match value {
        Value::Str(s) => any_value::Value::StringValue(s),
        Value::Int(i) => any_value::Value::IntValue(i),
    };
    KeyValue {
        key,
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// What the otel package's default error handler writes: Go's standard logger.
fn otel_error(message: &str) {
    crate::log::stdlib(message);
}

/// The batch span processor's queue.
pub(super) struct Queue {
    spans: mpsc::Sender<SpanData>,
    shutdown: mpsc::Sender<oneshot::Sender<Result<(), String>>>,
}

impl Queue {
    /// Queues a span, dropped when the queue is full.
    pub fn push(&self, span: SpanData) {
        let _ = self.spans.try_send(span);
    }

    /// Exports what is queued and stops.
    pub async fn shutdown(&self) -> Result<(), String> {
        let (done, flushed) = oneshot::channel();
        if self.shutdown.send(done).await.is_err() {
            return Ok(());
        }
        flushed.await.unwrap_or(Ok(()))
    }
}

/// `OTEL_BSP_*`: a positive integer, else the default.
fn bsp(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .map_or(default, |v| v as u64)
}

/// Starts the batch span processor and its exporter.
pub(super) fn start(protocol: Protocol, resource: Resource) -> Queue {
    let queue_size = bsp("OTEL_BSP_MAX_QUEUE_SIZE", 2048) as usize;
    let batch_size = (bsp("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 512) as usize).min(queue_size);
    let delay = Duration::from_millis(bsp("OTEL_BSP_SCHEDULE_DELAY", 5000));
    let export_timeout = Duration::from_millis(bsp("OTEL_BSP_EXPORT_TIMEOUT", 30000));
    let (spans, mut queued) = mpsc::channel::<SpanData>(queue_size);
    let (shutdown, mut stop) = mpsc::channel::<oneshot::Sender<Result<(), String>>>(1);
    let exporter = Exporter::new(protocol);
    tokio::spawn(async move {
        let mut batch = Vec::new();
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + delay, delay);
        loop {
            tokio::select! {
                span = queued.recv() => {
                    let Some(span) = span else { break };
                    batch.push(span);
                    if batch.len() >= batch_size {
                        let spans = std::mem::take(&mut batch);
                        log_failure(exporter.export(&resource, spans, export_timeout).await);
                    }
                }
                _ = ticks.tick() => {
                    if !batch.is_empty() {
                        let spans = std::mem::take(&mut batch);
                        log_failure(exporter.export(&resource, spans, export_timeout).await);
                    }
                }
                done = stop.recv() => {
                    while let Ok(span) = queued.try_recv() {
                        batch.push(span);
                    }
                    let mut result = Ok(());
                    for spans in std::mem::take(&mut batch).chunks(batch_size.max(1)) {
                        let exported = exporter.export(&resource, spans.to_vec(), export_timeout).await;
                        if let Err(e) = exported {
                            result = Err(e);
                        }
                    }
                    if let Some(done) = done {
                        let _ = done.send(result);
                    }
                    break;
                }
            }
        }
    });
    Queue { spans, shutdown }
}

fn log_failure(exported: Result<(), String>) {
    if let Err(e) = exported {
        otel_error(&e);
    }
}

/// The OTLP request for `spans`: one resource, the spans grouped by scope in the order the
/// scopes first come.
fn request(resource: &Resource, spans: Vec<SpanData>) -> ExportTraceServiceRequest {
    let mut scopes: Vec<ScopeSpans> = Vec::new();
    for span in spans {
        let scope = InstrumentationScope {
            name: span.scope.name.to_string(),
            version: span.scope.version.to_string(),
            ..Default::default()
        };
        let index = match scopes.iter().position(|s| s.scope.as_ref() == Some(&scope)) {
            Some(index) => index,
            None => {
                scopes.push(ScopeSpans {
                    scope: Some(scope),
                    ..Default::default()
                });
                scopes.len() - 1
            }
        };
        scopes[index].spans.push(otlp_span(span));
    }
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource.clone()),
            scope_spans: scopes,
            schema_url: RESOURCE_SCHEMA.to_string(),
        }],
    }
}

/// `tracetransform.span`: the status always present (unset, or error with its message); the
/// flags are the trace flags, with "is remote" known (`0x100`) and set (`0x200`) for a
/// remote parent.
fn otlp_span(span: SpanData) -> Span {
    let (code, message) = match span.status {
        Some(message) => (StatusCode::Error, message),
        None => (StatusCode::Unset, String::new()),
    };
    let kind = match span.kind {
        Kind::Internal => SpanKind::Internal,
        Kind::Server => SpanKind::Server,
    };
    Span {
        trace_id: span.context.trace_id.to_vec(),
        span_id: span.context.span_id.to_vec(),
        trace_state: span.context.trace_state,
        parent_span_id: span.parent_span_id.map_or_else(Vec::new, |id| id.to_vec()),
        flags: u32::from(span.context.flags) | 0x100 | if span.parent_remote { 0x200 } else { 0 },
        name: span.name,
        kind: kind as i32,
        start_time_unix_nano: span.start,
        end_time_unix_nano: span.end,
        attributes: span
            .attributes
            .into_iter()
            .map(|(k, v)| key_value(k, v))
            .collect(),
        status: Some(Status {
            message,
            code: code as i32,
        }),
        ..Default::default()
    }
}

/// The exporter's configuration, from `OTEL_EXPORTER_OTLP_*` then `OTEL_EXPORTER_OTLP_TRACES_*`
/// (which win), applied in the order Go applies them.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Config {
    /// `host:port` (gRPC: the endpoint's host and path joined).
    pub endpoint: String,
    /// HTTP's path, `/v1/traces` by default.
    pub path: String,
    pub insecure: bool,
    pub headers: Vec<(String, String)>,
    pub gzip: bool,
    pub timeout: Duration,
    /// PEM certificates to verify the collector with, instead of the system's.
    pub ca: Option<Vec<u8>>,
    /// A PEM client certificate and key.
    pub identity: Option<(Vec<u8>, Vec<u8>)>,
}

impl Config {
    pub fn from_env(protocol: Protocol) -> Self {
        Self::from_lookup(protocol, |name| std::env::var(name).ok())
    }

    /// The configuration from the variables `lookup` gives.
    fn from_lookup(protocol: Protocol, lookup: impl Fn(&str) -> Option<String>) -> Self {
        // The value of `OTEL_EXPORTER_OTLP_<name>`, trimmed, if not empty.
        let env = |name: &str| {
            let value = lookup(&format!("OTEL_EXPORTER_OTLP_{name}"))?;
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        };
        let mut config = Config {
            endpoint: match protocol {
                Protocol::Grpc => "localhost:4317".into(),
                Protocol::Http => "localhost:4318".into(),
            },
            path: "/v1/traces".into(),
            insecure: false,
            headers: Vec::new(),
            gzip: false,
            timeout: Duration::from_secs(10),
            ca: None,
            identity: None,
        };
        for (name, signal) in [("ENDPOINT", false), ("TRACES_ENDPOINT", true)] {
            let Some(value) = env(name) else { continue };
            let Some(url) = parse_url(&value) else {
                otel_error(&format!("parse url: {value}"));
                continue;
            };
            config.insecure = matches!(url.scheme.to_lowercase().as_str(), "http" | "unix");
            match protocol {
                Protocol::Grpc => config.endpoint = crate::go::join(&[&url.host, &url.path]),
                Protocol::Http => {
                    config.endpoint = url.host;
                    config.path = match signal {
                        false => crate::go::join(&[&url.path, "/v1/traces"]),
                        true if url.path.is_empty() => "/".into(),
                        true => url.path,
                    };
                }
            }
        }
        for name in ["CERTIFICATE", "TRACES_CERTIFICATE"] {
            if let Some(file) = env(name) {
                match std::fs::read(&file) {
                    Ok(pem) => config.ca = Some(pem),
                    Err(e) => otel_error(&format!("read tls ca cert file: {e}")),
                }
            }
        }
        for (cert, key) in [
            ("CLIENT_CERTIFICATE", "CLIENT_KEY"),
            ("TRACES_CLIENT_CERTIFICATE", "TRACES_CLIENT_KEY"),
        ] {
            if let (Some(cert), Some(key)) = (env(cert), env(key)) {
                match (std::fs::read(&cert), std::fs::read(&key)) {
                    (Ok(cert), Ok(key)) => config.identity = Some((cert, key)),
                    (Err(e), _) | (_, Err(e)) => otel_error(&format!("read tls client cert: {e}")),
                }
            }
        }
        for name in ["INSECURE", "TRACES_INSECURE"] {
            if let Some(value) = env(name) {
                config.insecure = value.eq_ignore_ascii_case("true");
            }
        }
        for name in ["HEADERS", "TRACES_HEADERS"] {
            if let Some(value) = env(name) {
                config.headers = headers(&value);
            }
        }
        for name in ["COMPRESSION", "TRACES_COMPRESSION"] {
            if let Some(value) = env(name) {
                config.gzip = value == "gzip";
            }
        }
        for name in ["TIMEOUT", "TRACES_TIMEOUT"] {
            if let Some(value) = env(name) {
                match crate::go::atoi(&value) {
                    Ok(ms) => config.timeout = Duration::from_millis(ms.max(0) as u64),
                    Err(e) => otel_error(&format!("parse duration: {e}")),
                }
            }
        }
        if protocol == Protocol::Http {
            config.path = clean_path(&config.path);
        }
        config
    }
}

/// `cleanPath`: an empty path (or `.`) is the default, a relative one made absolute.
fn clean_path(path: &str) -> String {
    match path.trim() {
        "" | "." => "/v1/traces".into(),
        path if path.starts_with('/') => path.into(),
        path => format!("/{path}"),
    }
}

/// `stringToHeader`: `key=value` pairs separated by commas, the key a token, the value
/// percent-decoded; trimmed. A later key replaces an earlier one.
fn headers(value: &str) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = Vec::new();
    for pair in value.split(',') {
        let Some((key, value)) = pair.split_once('=') else {
            otel_error(&format!("parse headers: missing '=': {pair}"));
            continue;
        };
        let key = key.trim();
        let token = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
        if key.is_empty() || !key.chars().all(token) {
            otel_error(&format!("parse headers: invalid header key: {key}"));
            continue;
        }
        let Some(value) = crate::go::unescape(value, false) else {
            otel_error(&format!("escape header value: {value}"));
            continue;
        };
        let value = value.trim().to_string();
        match headers.iter_mut().find(|(k, _)| k == key) {
            Some(existing) => existing.1 = value,
            None => headers.push((key.to_string(), value)),
        }
    }
    headers
}

/// The parts of a URL `url.Parse` gives that the exporter uses.
#[derive(Debug, Clone, PartialEq)]
struct Url {
    scheme: String,
    host: String,
    path: String,
}

/// `url.Parse`, for an endpoint: `scheme://host/path` (a query and fragment left out); a
/// value with no scheme is a path, which cannot have a colon in its first segment.
fn parse_url(value: &str) -> Option<Url> {
    let value = value.split('#').next().unwrap_or_default();
    let value = value.split('?').next().unwrap_or_default();
    let (scheme, rest) = match value.find(':') {
        Some(i)
            if i > 0
                && value[..i].starts_with(|c: char| c.is_ascii_alphabetic())
                && value[..i]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c)) =>
        {
            (&value[..i], &value[i + 1..])
        }
        Some(0) => return None,
        _ => ("", value),
    };
    match rest.strip_prefix("//") {
        Some(authority_path) => {
            let (authority, path) = match authority_path.find('/') {
                Some(i) => (&authority_path[..i], &authority_path[i..]),
                None => (authority_path, ""),
            };
            let host = authority.rsplit('@').next().unwrap_or_default();
            Some(Url {
                scheme: scheme.into(),
                host: host.into(),
                path: crate::go::unescape(path, false)?,
            })
        }
        // `scheme:opaque`: no host.
        None if !scheme.is_empty() => Some(Url {
            scheme: scheme.into(),
            host: String::new(),
            path: String::new(),
        }),
        None => {
            let first = rest.split('/').next().unwrap_or_default();
            if first.contains(':') {
                return None;
            }
            Some(Url {
                scheme: String::new(),
                host: String::new(),
                path: crate::go::unescape(rest, false)?,
            })
        }
    }
}

/// Sends batches to the collector.
struct Exporter {
    protocol: Protocol,
    config: Config,
    http: Option<HttpClient>,
    grpc: tokio::sync::OnceCell<Result<tonic::transport::Channel, String>>,
}

type HttpClient = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    http_body_util::Full<bytes::Bytes>,
>;

impl Exporter {
    fn new(protocol: Protocol) -> Self {
        let config = Config::from_env(protocol);
        let http = (protocol == Protocol::Http).then(|| http_client(&config));
        Exporter {
            protocol,
            config,
            http,
            grpc: tokio::sync::OnceCell::new(),
        }
    }

    async fn export(
        &self,
        resource: &Resource,
        spans: Vec<SpanData>,
        batch_timeout: Duration,
    ) -> Result<(), String> {
        let request = request(resource, spans);
        let timeout = self.config.timeout.min(batch_timeout);
        let sent = match self.protocol {
            Protocol::Grpc => tokio::time::timeout(timeout, self.export_grpc(request)).await,
            Protocol::Http => tokio::time::timeout(timeout, self.export_http(request)).await,
        };
        sent.unwrap_or_else(|_| Err("traces export: context deadline exceeded".into()))
    }

    async fn export_grpc(&self, request: ExportTraceServiceRequest) -> Result<(), String> {
        let channel = self
            .grpc
            .get_or_init(|| async { grpc_channel(&self.config) })
            .await
            .clone()?;
        let mut client = TraceServiceClient::new(channel);
        if self.config.gzip {
            client = client.send_compressed(tonic::codec::CompressionEncoding::Gzip);
        }
        let mut request = tonic::Request::new(request);
        request.set_timeout(self.config.timeout);
        for (key, value) in &self.config.headers {
            let (Ok(key), Ok(value)) = (
                tonic::metadata::MetadataKey::from_bytes(key.to_lowercase().as_bytes()),
                tonic::metadata::MetadataValue::try_from(value.as_str()),
            ) else {
                continue;
            };
            request.metadata_mut().insert(key, value);
        }
        client
            .export(request)
            .await
            .map(drop)
            .map_err(|status| format!("traces export: {}", status.message()))
    }

    async fn export_http(&self, request: ExportTraceServiceRequest) -> Result<(), String> {
        let scheme = if self.config.insecure {
            "http"
        } else {
            "https"
        };
        let url = format!("{scheme}://{}{}", self.config.endpoint, self.config.path);
        let body = prost::Message::encode_to_vec(&request);
        let mut builder = hyper::Request::post(&url)
            .header("User-Agent", USER_AGENT)
            .header("Content-Type", "application/x-protobuf");
        for (key, value) in &self.config.headers {
            builder = builder.header(key.as_str(), value.as_str());
        }
        let body = if self.config.gzip {
            use std::io::Write;
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            let _ = gz.write_all(&body);
            builder = builder.header("Content-Encoding", "gzip");
            gz.finish().unwrap_or_default()
        } else {
            body
        };
        let request = builder
            .body(http_body_util::Full::new(bytes::Bytes::from(body)))
            .map_err(|e| format!("traces export: {e}"))?;
        let client = self.http.as_ref().expect("an HTTP exporter");
        let response = client
            .request(request)
            .await
            .map_err(|e| format!("traces export: Post \"{url}\": {e}"))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .map(|b| String::from_utf8_lossy(&b.to_bytes()).trim().to_string())
            .unwrap_or_default();
        let body = if body.is_empty() {
            "(empty)".into()
        } else {
            body
        };
        Err(format!(
            "traces export: failed to send to {url}: {status} (body: {body})"
        ))
    }
}

/// This exporter's `User-Agent`.
const USER_AGENT: &str = concat!("ratelimit/", env!("CARGO_PKG_VERSION"));

/// The TLS configuration: the system's roots or `CERTIFICATE`, and the client certificate.
fn tls_config(config: &Config) -> Result<rustls::ClientConfig, String> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    match &config.ca {
        Some(pem) => {
            for cert in rustls::pki_types::CertificateDer::pem_slice_iter(pem) {
                let cert = cert.map_err(|e| e.to_string())?;
                roots.add(cert).map_err(|e| e.to_string())?;
            }
        }
        None => {
            for cert in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(cert);
            }
        }
    }
    let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_root_certificates(roots);
    match &config.identity {
        Some((cert, key)) => {
            let certs = rustls::pki_types::CertificateDer::pem_slice_iter(cert)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            let key =
                rustls::pki_types::PrivateKeyDer::from_pem_slice(key).map_err(|e| e.to_string())?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| e.to_string())
        }
        None => Ok(builder.with_no_client_auth()),
    }
}

fn http_client(config: &Config) -> HttpClient {
    let tls = tls_config(config).unwrap_or_else(|e| {
        otel_error(&format!("create tls config: {e}"));
        tls_config(&Config {
            ca: None,
            identity: None,
            ..config.clone()
        })
        .expect("a TLS configuration with the system's roots")
    });
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .build();
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build(connector)
}

/// Whether gRPC goes over TLS: unless insecure, and always with a certificate configured
/// (Go's TLS credentials win over `INSECURE` and an `http` endpoint).
fn grpc_tls(config: &Config) -> bool {
    !config.insecure || config.ca.is_some() || config.identity.is_some()
}

/// A lazily connected channel to the collector, over TLS as [`grpc_tls`] says.
fn grpc_channel(config: &Config) -> Result<tonic::transport::Channel, String> {
    let tls = grpc_tls(config);
    let scheme = if tls { "https" } else { "http" };
    let mut endpoint =
        tonic::transport::Endpoint::from_shared(format!("{scheme}://{}", config.endpoint))
            .map_err(|e| format!("traces export: {e}"))?
            .user_agent(USER_AGENT)
            .map_err(|e| format!("traces export: {e}"))?;
    if tls {
        let mut tls = tonic::transport::ClientTlsConfig::new();
        tls = match &config.ca {
            Some(pem) => tls.ca_certificate(tonic::transport::Certificate::from_pem(pem)),
            None => tls.with_native_roots(),
        };
        if let Some((cert, key)) = &config.identity {
            tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| format!("traces export: {e}"))?;
    }
    Ok(endpoint.connect_lazy())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_parse_like_go() {
        let url = |s| parse_url(s).map(|u| (u.scheme, u.host, u.path));
        assert_eq!(
            url("http://h:1/p?q#f"),
            Some(("http".into(), "h:1".into(), "/p".into()))
        );
        assert_eq!(
            url("localhost:4318"),
            Some(("localhost".into(), String::new(), String::new()))
        );
        assert_eq!(url("127.0.0.1:4318"), None);
        assert_eq!(url(":x"), None);
    }

    #[test]
    fn a_certificate_makes_grpc_use_tls() {
        let insecure = Config {
            insecure: true,
            ..Config::from_env(Protocol::Grpc)
        };
        assert!(!grpc_tls(&insecure));
        assert!(grpc_tls(&Config {
            ca: Some(Vec::new()),
            ..insecure.clone()
        }));
        assert!(grpc_tls(&Config {
            identity: Some((Vec::new(), Vec::new())),
            ..insecure
        }));
    }

    #[test]
    fn variables_apply_in_gos_order() {
        let config = |protocol, vars: &[(&str, &str)]| {
            let vars: Vec<(String, String)> = vars
                .iter()
                .map(|(k, v)| (format!("OTEL_EXPORTER_OTLP_{k}"), v.to_string()))
                .collect();
            Config::from_lookup(protocol, |name| {
                vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone())
            })
        };
        let default = config(Protocol::Http, &[]);
        assert_eq!(
            (
                default.endpoint.as_str(),
                default.path.as_str(),
                default.insecure
            ),
            ("localhost:4318", "/v1/traces", false)
        );
        assert_eq!(config(Protocol::Grpc, &[]).endpoint, "localhost:4317");
        // The signal's endpoint wins, its path as given; `INSECURE` decides over the scheme.
        let http = config(
            Protocol::Http,
            &[
                ("ENDPOINT", "http://a:1/base"),
                ("TRACES_ENDPOINT", "https://b:2/custom"),
                ("INSECURE", " TRUE "),
                ("TIMEOUT", "250"),
                ("TRACES_TIMEOUT", "x"),
                ("HEADERS", "a=1"),
                ("TRACES_HEADERS", "b=2"),
                ("COMPRESSION", "gzip"),
                ("TRACES_COMPRESSION", "GZIP"),
            ],
        );
        assert_eq!(
            (http.endpoint.as_str(), http.path.as_str()),
            ("b:2", "/custom")
        );
        assert!(http.insecure);
        assert_eq!(http.timeout, Duration::from_millis(250));
        assert_eq!(http.headers, vec![("b".to_string(), "2".to_string())]);
        assert!(!http.gzip);
        let base = config(Protocol::Http, &[("ENDPOINT", "https://a:1/base/")]);
        assert_eq!(
            (base.path.as_str(), base.insecure),
            ("/base/v1/traces", false)
        );
        let grpc = config(
            Protocol::Grpc,
            &[("ENDPOINT", "unix://h:3/p"), ("TRACES_INSECURE", "false")],
        );
        assert_eq!((grpc.endpoint.as_str(), grpc.insecure), ("h:3/p", false));
        assert!(config(Protocol::Grpc, &[("ENDPOINT", "unix://h:3")]).insecure);
    }

    #[test]
    fn paths_are_cleaned_like_go() {
        assert_eq!(clean_path(" "), "/v1/traces");
        assert_eq!(clean_path("."), "/v1/traces");
        assert_eq!(clean_path("x/y"), "/x/y");
        assert_eq!(clean_path("/"), "/");
    }

    #[test]
    fn headers_parse_like_go() {
        assert_eq!(
            headers(" a = b%20c ,bad,d=e,a=f"),
            vec![("a".into(), "f".into()), ("d".into(), "e".into())]
        );
    }
}
