//! The service's HTTP server on `HOST:PORT` (`src/server/server_impl.go`): a gorilla/mux
//! router with `/healthcheck` and `/json`.

use crate::goserver::{Answer, Request};
use crate::health::HealthChecker;
use crate::service::{RateLimitCache, RateLimitService};
use bytes::Bytes;
use http_body_util::BodyExt;
use ratelimit_proto::envoy::service::ratelimit::v3::rate_limit_response::Code;
use std::sync::Arc;

/// The router on the decoded path (gorilla/mux matches it, not the escaped one). Go's
/// strings hold any bytes: each decoded byte is kept as one char, which `clean` and matching
/// treat alike, and turned back into its byte when escaped.
fn respond(health: &HealthChecker, request: &Request) -> Routed {
    let uri = request.uri();
    // A bad escape was refused before routing (`goserver`).
    let path = crate::go::unescape_bytes(uri.path(), false).unwrap_or_default();
    let path: String = path.into_iter().map(char::from).collect();
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
    let clean = crate::mux::clean_path(path);
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
async fn json<C: RateLimitCache>(
    service: &RateLimitService<C>,
    body: Result<Bytes, String>,
) -> Answer {
    // Each failure is logged, then answered alike.
    let bad_request = |error: String| {
        crate::log::warn(&format!("error: {error}"));
        Answer::error(400, "Bad Request")
    };
    let request = match body.and_then(|body| crate::protojson::decode_request(&body)) {
        Ok(request) => request,
        Err(error) => return bad_request(error),
    };
    let response = match service.should_rate_limit(&request).await {
        Ok(response) => response,
        Err(error) => return bad_request(error.to_string()),
    };
    crate::debug!("resp:{}", crate::prototext::response(&response));
    let body = match crate::protojson::encode_response(&response) {
        Ok(body) => body,
        Err(error) => {
            crate::log::error(&format!("error marshaling proto3 to json: {error}"));
            return Answer::error(500, "Internal Server Error");
        }
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
    let handler = move |request: Request| {
        let (health, service) = (health.clone(), service.clone());
        async move {
            match respond(&health, &request) {
                Routed::Answer(answer) => answer,
                Routed::Json => {
                    // Upstream reads any length; past the size gRPC refuses, it is refused.
                    let body = http_body_util::Limited::new(request.into_body(), MAX_JSON_BODY);
                    let body = body.collect().await.map_err(|e| e.to_string());
                    json(&service, body.map(|body| body.to_bytes())).await
                }
            }
        }
    };
    crate::goserver::serve(listener, handler, stop).await;
}
