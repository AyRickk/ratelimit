//! The service's HTTP server on `HOST:PORT` (`src/server/server_impl.go`): a gorilla/mux
//! router with `/healthcheck` and `/json`.

use crate::health::HealthChecker;
use crate::service::{RateLimitCache, RateLimitService};
use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
use std::sync::Arc;

/// An answer, before it becomes a hyper response.
struct Answer {
    status: u16,
    /// A reason phrase other than the status's own.
    reason: Option<String>,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl Answer {
    fn new(status: u16, headers: Vec<(&'static str, String)>, body: &[u8]) -> Self {
        Self {
            status,
            reason: None,
            headers,
            body: body.to_vec(),
        }
    }

    /// `http.Error`: the status's text as plain text.
    fn error(status: u16, text: &str) -> Self {
        let headers = vec![
            ("content-type", "text/plain; charset=utf-8".into()),
            ("x-content-type-options", "nosniff".into()),
        ];
        Self::new(status, headers, format!("{text}\n").as_bytes())
    }

    /// Go's server refusing a request before any handler (`conn.serve`): the connection is
    /// closed, and with a reason, it follows the status in the status line and the body.
    fn bad_request(reason: Option<&str>) -> Self {
        let text = match reason {
            Some(reason) => format!("Bad Request: {reason}"),
            None => "Bad Request".into(),
        };
        let headers = vec![
            ("content-type", "text/plain; charset=utf-8".into()),
            ("connection", "close".into()),
        ];
        Self {
            reason: reason.map(|_| text.clone()),
            ..Self::new(400, headers, format!("400 {text}").as_bytes())
        }
    }
}

/// gorilla/mux's `cleanPath`: `path.Clean` of a rooted path, keeping a trailing slash.
fn clean_path(path: &str) -> String {
    if path.is_empty() {
        return "/".into();
    }
    let rooted = match path.starts_with('/') {
        true => path.to_string(),
        false => format!("/{path}"),
    };
    let mut clean = crate::go::clean(&rooted);
    if rooted.ends_with('/') && clean != "/" {
        clean.push('/');
    }
    clean
}

/// What Go's server checks before routing (`conn.readRequest`, `url.ParseRequestURI`), then
/// `OPTIONS *` (answered by the server itself), then the router.
fn respond<B>(health: &HealthChecker, request: &hyper::Request<B>) -> Routed {
    let uri = request.uri();
    // Go's strings hold any bytes: each decoded byte is kept as one char, which `clean` and
    // matching treat alike, and turned back into its byte when escaped.
    let Some(path) = crate::go::unescape_bytes(uri.path(), false) else {
        return Routed::Answer(Answer::bad_request(None));
    };
    let path: String = path.into_iter().map(char::from).collect();
    let hosts = request.headers().get_all(hyper::header::HOST);
    let mut hosts = hosts.iter();
    let (first_host, more_hosts) = (hosts.next(), hosts.next().is_some());
    if request.version() >= hyper::Version::HTTP_11
        && first_host.is_none()
        && request.method() != hyper::Method::CONNECT
    {
        return Routed::Answer(Answer::bad_request(Some("missing required Host header")));
    }
    if let Some(host) = first_host
        && !more_hosts
        && !host
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"!$%&'()*+,-.:;=[]_~".contains(b))
    {
        return Routed::Answer(Answer::bad_request(Some("malformed Host header")));
    }
    if request.method() == hyper::Method::OPTIONS && uri == "*" {
        return Routed::Answer(Answer::new(200, Vec::new(), b""));
    }
    route(health, uri, &path)
}

/// The largest `/json` body read: grpc-go's default largest message.
const MAX_JSON_BODY: usize = 4 << 20;

/// Where a request goes: answered at once, or to `/json`, which reads its body.
enum Routed {
    Answer(Answer),
    Json,
}

/// The router on the decoded `path`: a path that is not clean is redirected (301, no body)
/// to the URL with the clean path, then `/healthcheck` and `/json` answer, and anything else
/// is not found. Any method is served.
fn route(health: &HealthChecker, uri: &hyper::Uri, path: &str) -> Routed {
    let clean = clean_path(path);
    if clean != path {
        // `url.URL.String`: the scheme and host of an absolute-form target are kept.
        let mut location = match (uri.scheme_str(), uri.authority()) {
            (Some(scheme), Some(authority)) => format!("{scheme}://{authority}"),
            _ => String::new(),
        };
        let bytes: Vec<u8> = clean.chars().map(|c| c as u8).collect();
        location += &crate::go::escape_path(&bytes);
        if let Some(query) = uri.query() {
            location = format!("{location}?{query}");
        }
        return Routed::Answer(Answer::new(301, vec![("location", location)], b""));
    }
    let text = || vec![("content-type", "text/plain; charset=utf-8".to_string())];
    Routed::Answer(match path {
        "/healthcheck" if health.is_ok() => Answer::new(200, text(), b"OK"),
        "/healthcheck" => Answer::new(500, Vec::new(), b""),
        "/json" => return Routed::Json,
        _ => Answer::error(404, "404 page not found"),
    })
}

/// `NewJsonHandler`: the body as a `RateLimitRequest` in protojson, answered by the service
/// (400 for any error) in protojson: 429 when over the limit, 500 for an unknown code or a
/// response protojson cannot write.
async fn json<C: RateLimitCache>(service: &RateLimitService<C>, body: Option<Bytes>) -> Answer {
    let bad_request = || Answer::error(400, "Bad Request");
    let Some(request) = body.and_then(|body| crate::protojson::decode_request(&body).ok()) else {
        return bad_request();
    };
    let Ok(response) = service.should_rate_limit(&request).await else {
        return bad_request();
    };
    let Ok(body) = crate::protojson::encode_response(&response) else {
        return Answer::error(500, "Internal Server Error");
    };
    let status = match response.overall_code() {
        Code::Unknown => 500,
        Code::OverLimit => 429,
        Code::Ok => 200,
    };
    let headers = vec![("content-type", "application/json".to_string())];
    Answer::new(status, headers, body.as_bytes())
}

/// Serves HTTP/1 requests from `listener` until `stop` resolves, then closes at once (Go's
/// `http.Server.Close`).
pub async fn serve<C: RateLimitCache>(
    listener: tokio::net::TcpListener,
    health: Arc<HealthChecker>,
    service: Arc<RateLimitService<C>>,
    stop: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(stop);
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // Finished connections are reaped, or the set would keep each one.
            Some(_) = connections.join_next() => continue,
            () = &mut stop => break,
        };
        let Ok((stream, _)) = accepted else {
            continue;
        };
        let (health, service) = (health.clone(), service.clone());
        connections.spawn(async move {
            let handler = hyper::service::service_fn(move |request| {
                let (health, service) = (health.clone(), service.clone());
                async move {
                    let response = respond_to(&health, &service, request).await;
                    Ok::<_, std::convert::Infallible>(response)
                }
            });
            let io = hyper_util::rt::TokioIo::new(stream);
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, handler)
                .await;
        });
    }
    connections.abort_all();
}

/// The response to `request`.
async fn respond_to<C: RateLimitCache>(
    health: &HealthChecker,
    service: &RateLimitService<C>,
    request: hyper::Request<hyper::body::Incoming>,
) -> hyper::Response<Full<Bytes>> {
    let answer = match respond(health, &request) {
        Routed::Answer(answer) => answer,
        Routed::Json => {
            // Upstream reads any length; past the size gRPC refuses, the body is refused.
            let body = http_body_util::Limited::new(request.into_body(), MAX_JSON_BODY);
            let body = body.collect().await.ok();
            json(service, body.map(|body| body.to_bytes())).await
        }
    };
    let mut response = hyper::Response::builder().status(answer.status);
    if let Some(reason) = answer.reason {
        let reason = hyper::ext::ReasonPhrase::try_from(reason.into_bytes());
        response = response.extension(reason.expect("a reason phrase from Go's"));
    }
    for (name, value) in answer.headers {
        response = response.header(name, value);
    }
    response.body(Full::new(Bytes::from(answer.body))).unwrap()
}
