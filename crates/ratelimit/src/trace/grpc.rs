//! otelgrpc's server handler (v0.70.0): a server span per gRPC call, named after the method,
//! under the caller's trace, with the semantic conventions `OTEL_SEMCONV_STABILITY_OPT_IN`
//! chooses, the server's address, and the call's final status.

use super::{Kind, Scope, Span, Value};
use std::pin::Pin;
use std::task::{Context, Poll};

/// otelgrpc's tracer.
pub const SCOPE: Scope = Scope {
    name: "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc",
    version: "0.70.0",
};

/// The attributes of the method: the current conventions, the previous ones (`rpc/old`), or
/// both (`rpc/dup`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Conventions {
    Current,
    Previous,
    Both,
}

/// `parseSemconvMode`: the first of `rpc/dup` or `rpc/old` in the comma-separated list.
fn conventions() -> Conventions {
    let value = std::env::var("OTEL_SEMCONV_STABILITY_OPT_IN").unwrap_or_default();
    for part in value.split(',') {
        match part.trim() {
            "rpc/dup" => return Conventions::Both,
            "rpc/old" => return Conventions::Previous,
            _ => {}
        }
    }
    Conventions::Current
}

/// The layer, for the gRPC server; `uds` is the socket path the server listens on, if any.
/// With tracing off, calls go through untouched.
#[derive(Debug, Clone)]
pub struct Layer {
    conventions: Conventions,
    uds: Option<String>,
}

impl Layer {
    pub fn new(uds: Option<String>) -> Self {
        Layer {
            conventions: conventions(),
            uds,
        }
    }
}

impl<S> tower::Layer<S> for Layer {
    type Service = Traced<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Traced {
            inner,
            layer: self.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Traced<S> {
    inner: S,
    layer: Layer,
}

/// The span's name and the method's attributes (`ParseFullMethod`, `ParseFullMethodOld`).
fn method(full: &str, conventions: Conventions) -> (String, Vec<(String, Value)>) {
    let Some(name) = full.strip_prefix('/') else {
        return (full.to_string(), Vec::new());
    };
    let string = |k: &str, v: &str| (k.to_string(), Value::Str(v.to_string()));
    let current = vec![string("rpc.method", name)];
    let previous = match name.split_once('/') {
        Some((service, method)) => {
            let method = method.split('/').next().unwrap_or_default();
            vec![
                string("rpc.system", "grpc"),
                string("rpc.service", service),
                string("rpc.method", method),
            ]
        }
        None => vec![string("rpc.system", "grpc")],
    };
    let system_name = string("rpc.system.name", "grpc");
    let attributes = match conventions {
        Conventions::Current => [current, vec![system_name]].concat(),
        Conventions::Previous => previous,
        // The current `rpc.method` replaces the previous one.
        Conventions::Both => {
            let mut attributes = previous;
            for (key, value) in current {
                match attributes.iter_mut().find(|(k, _)| *k == key) {
                    Some(existing) => existing.1 = value,
                    None => attributes.push((key, value)),
                }
            }
            attributes.push(system_name);
            attributes
        }
    };
    (name.to_string(), attributes)
}

/// `serverAddrAttrs` of the connection's local address: the host and the port, or the
/// socket path.
fn server_address(address: &str) -> Vec<(String, Value)> {
    let Ok((host, port)) = crate::go::split_host_port(address) else {
        return vec![("server.address".into(), Value::Str(address.into()))];
    };
    let mut attributes = vec![("server.address".into(), Value::Str(host))];
    if let Ok(port) = port.parse::<i64>() {
        attributes.push(("server.port".into(), Value::Int(port)));
    }
    attributes
}

impl<S, B, R> tower::Service<http::Request<B>> for Traced<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    R: http_body::Body + Unpin,
{
    type Response = http::Response<TracedBody<R>>;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        if !super::enabled() {
            let response = self.inner.call(request);
            return Box::pin(async move {
                let (parts, body) = response.await?.into_parts();
                Ok(http::Response::from_parts(
                    parts,
                    TracedBody {
                        body,
                        span: None,
                        status: None,
                    },
                ))
            });
        }
        let parent = super::propagation::extract(request.headers());
        let (name, attributes) = method(request.uri().path(), self.layer.conventions);
        let mut span = super::start(SCOPE, &name, Kind::Server, parent.as_ref(), || attributes);
        let local = match &self.layer.uds {
            Some(path) => Some(path.clone()),
            None => request
                .extensions()
                .get::<tonic::transport::server::TcpConnectInfo>()
                .and_then(|info| info.local_addr())
                .map(|address| match address.ip() {
                    // Go prints an IPv4-mapped address as IPv4.
                    std::net::IpAddr::V6(ip) => match ip.to_ipv4_mapped() {
                        Some(v4) => std::net::SocketAddr::from((v4, address.port())).to_string(),
                        None => address.to_string(),
                    },
                    std::net::IpAddr::V4(_) => address.to_string(),
                }),
        };
        if let Some(local) = local {
            for (key, value) in server_address(&local) {
                span.set_attribute(&key, value);
            }
        }
        if let Some(context) = span.context() {
            request.extensions_mut().insert(context.clone());
        }
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            let (parts, body) = response.into_parts();
            // A trailers-only answer has its status in the headers.
            let status = grpc_status(&parts.headers);
            Ok(http::Response::from_parts(
                parts,
                TracedBody {
                    body,
                    span: Some(span),
                    status,
                },
            ))
        })
    }
}

/// The `grpc-status` and `grpc-message` of headers or trailers.
fn grpc_status(headers: &http::HeaderMap) -> Option<(i32, String)> {
    let code = headers.get("grpc-status")?.to_str().ok()?.parse().ok()?;
    let message = headers
        .get("grpc-message")
        .and_then(|m| m.to_str().ok())
        .map(|m| crate::go::unescape(m, false).unwrap_or_else(|| m.to_string()))
        .unwrap_or_default();
    Some((code, message))
}

/// `canonicalString` of a gRPC status code.
fn canonical(code: i32) -> String {
    const NAMES: [&str; 17] = [
        "OK",
        "CANCELLED",
        "UNKNOWN",
        "INVALID_ARGUMENT",
        "DEADLINE_EXCEEDED",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "PERMISSION_DENIED",
        "RESOURCE_EXHAUSTED",
        "FAILED_PRECONDITION",
        "ABORTED",
        "OUT_OF_RANGE",
        "UNIMPLEMENTED",
        "INTERNAL",
        "UNAVAILABLE",
        "DATA_LOSS",
        "UNAUTHENTICATED",
    ];
    usize::try_from(code)
        .ok()
        .and_then(|c| NAMES.get(c))
        .map_or_else(|| format!("CODE({code})"), |name| name.to_string())
}

/// The answer's body, which ends the span once the call's status is known.
pub struct TracedBody<B> {
    body: B,
    span: Option<Span>,
    status: Option<(i32, String)>,
}

impl<B> TracedBody<B> {
    /// `stats.End`: the status code attribute, and an error status for the codes that are
    /// the server's fault (`serverStatus`).
    fn end(&mut self) {
        let Some(mut span) = self.span.take() else {
            return;
        };
        let (code, message) = self.status.take().unwrap_or((0, String::new()));
        if matches!(code, 2 | 4 | 12 | 13 | 14 | 15) {
            span.set_error(&message);
        }
        span.set_attribute("rpc.response.status_code", Value::Str(canonical(code)));
    }
}

impl<B> Drop for TracedBody<B> {
    fn drop(&mut self) {
        self.end();
    }
}

impl<B: http_body::Body + Unpin> http_body::Body for TracedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.body).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(status) = frame.trailers_ref().and_then(grpc_status) {
                    self.status = Some(status);
                    self.end();
                }
            }
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => self.end(),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn methods_are_named_like_otelgrpc() {
        let method_name = "/envoy.service.ratelimit.v3.RateLimitService/ShouldRateLimit";
        let (name, attributes) = method(method_name, Conventions::Both);
        assert_eq!(name, &method_name[1..]);
        let keys: Vec<&str> = attributes.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            ["rpc.system", "rpc.service", "rpc.method", "rpc.system.name"]
        );
        assert_eq!(attributes[2].1, Value::Str(method_name[1..].into()));
        assert_eq!(canonical(14), "UNAVAILABLE");
        assert_eq!(canonical(42), "CODE(42)");
    }
}
