//! The service's HTTP server on `HOST:PORT` (`src/server/server_impl.go`): a gorilla/mux
//! router with `/healthcheck`.

use crate::health::HealthChecker;
use bytes::Bytes;
use http_body_util::Full;
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
fn respond<B>(health: &HealthChecker, request: &hyper::Request<B>) -> Answer {
    let uri = request.uri();
    // Go's strings hold any bytes: each decoded byte is kept as one char, which `clean` and
    // matching treat alike, and turned back into its byte when escaped.
    let Some(path) = crate::go::unescape_bytes(uri.path(), false) else {
        return Answer::bad_request(None);
    };
    let path: String = path.into_iter().map(char::from).collect();
    let hosts = request.headers().get_all(hyper::header::HOST);
    let mut hosts = hosts.iter();
    let (first_host, more_hosts) = (hosts.next(), hosts.next().is_some());
    if request.version() >= hyper::Version::HTTP_11
        && first_host.is_none()
        && request.method() != hyper::Method::CONNECT
    {
        return Answer::bad_request(Some("missing required Host header"));
    }
    if let Some(host) = first_host
        && !more_hosts
        && !host
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"!$%&'()*+,-.:;=[]_~".contains(b))
    {
        return Answer::bad_request(Some("malformed Host header"));
    }
    if request.method() == hyper::Method::OPTIONS && uri == "*" {
        return Answer::new(200, Vec::new(), b"");
    }
    route(health, uri, &path)
}

/// The router on the decoded `path`: a path that is not clean is redirected (301, no body)
/// to the URL with the clean path, then `/healthcheck` answers, and anything else is not
/// found. Any method is served.
fn route(health: &HealthChecker, uri: &hyper::Uri, path: &str) -> Answer {
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
        return Answer::new(301, vec![("location", location)], b"");
    }
    let text = || vec![("content-type", "text/plain; charset=utf-8".to_string())];
    match path {
        "/healthcheck" if health.is_ok() => Answer::new(200, text(), b"OK"),
        "/healthcheck" => Answer::new(500, Vec::new(), b""),
        _ => {
            let mut headers = text();
            headers.push(("x-content-type-options", "nosniff".into()));
            Answer::new(404, headers, b"404 page not found\n")
        }
    }
}

/// Serves HTTP/1 requests from `listener` until `stop` resolves, then closes at once (Go's
/// `http.Server.Close`).
pub async fn serve(
    listener: tokio::net::TcpListener,
    health: Arc<HealthChecker>,
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
        let health = health.clone();
        connections.spawn(async move {
            let service = hyper::service::service_fn(move |request: hyper::Request<_>| {
                let answer = respond(&health, &request);
                let mut response = hyper::Response::builder().status(answer.status);
                if let Some(reason) = answer.reason {
                    let reason = hyper::ext::ReasonPhrase::try_from(reason.into_bytes());
                    response = response.extension(reason.expect("a reason phrase from Go's"));
                }
                for (name, value) in answer.headers {
                    response = response.header(name, value);
                }
                let response = response.body(Full::new(Bytes::from(answer.body))).unwrap();
                async move { Ok::<_, std::convert::Infallible>(response) }
            });
            let io = hyper_util::rt::TokioIo::new(stream);
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await;
        });
    }
    connections.abort_all();
}
