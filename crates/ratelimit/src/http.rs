//! The service's HTTP server on `HOST:PORT` (`src/server/server_impl.go`): a gorilla/mux
//! router with `/healthcheck`.

use crate::health::HealthChecker;
use bytes::Bytes;
use http_body_util::Full;
use std::sync::Arc;

/// An answer, before it becomes a hyper response.
struct Answer {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
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

/// The router: a path that is not clean is redirected (301, no body), then `/healthcheck`
/// answers, and anything else is not found. Any method is served.
fn route(health: &HealthChecker, raw_path: &str, query: Option<&str>) -> Answer {
    // Go matches the decoded path.
    let path = crate::go::unescape(raw_path, false).unwrap_or_else(|| raw_path.to_string());
    let clean = clean_path(&path);
    if clean != path {
        let mut location = crate::go::escape_path(&clean);
        if let Some(query) = query {
            location = format!("{location}?{query}");
        }
        return Answer {
            status: 301,
            headers: vec![("location", location)],
            body: Vec::new(),
        };
    }
    match path.as_str() {
        "/healthcheck" if health.is_ok() => Answer {
            status: 200,
            headers: vec![("content-type", "text/plain; charset=utf-8".into())],
            body: b"OK".to_vec(),
        },
        "/healthcheck" => Answer {
            status: 500,
            headers: Vec::new(),
            body: Vec::new(),
        },
        _ => Answer {
            status: 404,
            headers: vec![
                ("content-type", "text/plain; charset=utf-8".into()),
                ("x-content-type-options", "nosniff".into()),
            ],
            body: b"404 page not found\n".to_vec(),
        },
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
            () = &mut stop => break,
        };
        let Ok((stream, _)) = accepted else {
            continue;
        };
        let health = health.clone();
        connections.spawn(async move {
            let service = hyper::service::service_fn(move |request: hyper::Request<_>| {
                let uri = request.uri();
                let answer = route(&health, uri.path(), uri.query());
                let mut response = hyper::Response::builder().status(answer.status);
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
