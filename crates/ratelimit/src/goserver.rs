//! What Go's `net/http` server does around its handlers, for the service's HTTP servers: the
//! checks before routing, `OPTIONS *`, the helpers handlers answer with, and serving
//! connections.

use bytes::Bytes;
use http_body_util::Full;

pub type Request = hyper::Request<hyper::body::Incoming>;

/// An answer, before it becomes a hyper response.
pub struct Answer {
    pub status: u16,
    /// A reason phrase other than the status's own.
    pub reason: Option<String>,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

impl Answer {
    pub fn new(status: u16, headers: Vec<(&'static str, String)>, body: &[u8]) -> Self {
        Self {
            status,
            reason: None,
            headers,
            body: body.to_vec(),
        }
    }

    /// A handler that sets no content type: Go sniffs one from the body, if any.
    pub fn sniffed(status: u16, mut headers: Vec<(&'static str, String)>, body: &[u8]) -> Self {
        if !body.is_empty() {
            let content_type = crate::go::detect_content_type(body);
            headers.push(("content-type", content_type.into()));
        }
        Self::new(status, headers, body)
    }

    /// `http.Error`: the text, as plain text.
    pub fn error(status: u16, text: &str) -> Self {
        let headers = vec![
            ("content-type", "text/plain; charset=utf-8".into()),
            ("x-content-type-options", "nosniff".into()),
        ];
        Self::new(status, headers, format!("{text}\n").as_bytes())
    }

    /// `http.Redirect` with 307 (Go's `ServeMux`), for a location that is already an
    /// escaped absolute path: an HTML link for a GET.
    pub fn redirect(location: &str, method: &hyper::Method) -> Self {
        let mut headers = vec![("location", location.to_string())];
        let mut body = Vec::new();
        if matches!(*method, hyper::Method::GET | hyper::Method::HEAD) {
            headers.push(("content-type", "text/html; charset=utf-8".into()));
        }
        if *method == hyper::Method::GET {
            let escaped = location
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&#34;")
                .replace('\'', "&#39;");
            body = format!("<a href=\"{escaped}\">Temporary Redirect</a>.\n\n").into_bytes();
        }
        Self::new(307, headers, &body)
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

    fn into_response(self) -> hyper::Response<Full<Bytes>> {
        let mut response = hyper::Response::builder().status(self.status);
        if let Some(reason) = self.reason {
            let reason = hyper::ext::ReasonPhrase::try_from(reason.into_bytes());
            response = response.extension(reason.expect("a reason phrase from Go's"));
        }
        for (name, value) in self.headers {
            response = response.header(name, value);
        }
        response.body(Full::new(Bytes::from(self.body))).unwrap()
    }
}

/// What Go's server answers itself: requests it refuses before routing (`conn.readRequest`,
/// `url.ParseRequestURI`), and `OPTIONS *`.
fn answered_by_server(request: &Request) -> Option<Answer> {
    let uri = request.uri();
    if crate::go::unescape_bytes(uri.path(), false).is_none() {
        return Some(Answer::bad_request(None));
    }
    let hosts = request.headers().get_all(hyper::header::HOST);
    let mut hosts = hosts.iter();
    let (first_host, more_hosts) = (hosts.next(), hosts.next().is_some());
    if request.version() >= hyper::Version::HTTP_11
        && first_host.is_none()
        && request.method() != hyper::Method::CONNECT
    {
        return Some(Answer::bad_request(Some("missing required Host header")));
    }
    if let Some(host) = first_host
        && !more_hosts
        && !host
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"!$%&'()*+,-.:;=[]_~".contains(b))
    {
        return Some(Answer::bad_request(Some("malformed Host header")));
    }
    if request.method() == hyper::Method::OPTIONS && uri == "*" {
        return Some(Answer::new(200, Vec::new(), b""));
    }
    None
}

/// Serves HTTP/1 requests from `listener` with `handler` until `stop` resolves, then closes
/// at once (`http.Server.Close`).
pub async fn serve<H, F>(
    listener: tokio::net::TcpListener,
    handler: H,
    stop: impl std::future::Future<Output = ()>,
) where
    H: Fn(Request) -> F + Clone + Send + Sync + 'static,
    F: std::future::Future<Output = Answer> + Send + 'static,
{
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
        let handler = handler.clone();
        connections.spawn(async move {
            let service = hyper::service::service_fn(move |request: Request| {
                let handler = handler.clone();
                async move {
                    let answer = match answered_by_server(&request) {
                        Some(answer) => answer,
                        None => handler(request).await,
                    };
                    Ok::<_, std::convert::Infallible>(answer.into_response())
                }
            });
            let io = hyper_util::rt::TokioIo::new(stream);
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await;
        });
    }
    connections.abort_all();
}

/// `url.URL.EscapedPath` of a request target's path: as sent when it is a valid escaping,
/// else the unescaped path escaped again (`None` for a bad `%` escape).
pub fn escaped_path(raw: &str) -> Option<String> {
    let decoded = crate::go::unescape_bytes(raw, false)?;
    let valid = raw
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"$&+,-./:;=@_~!'()*[]%".contains(&b));
    Some(match valid {
        true => raw.to_string(),
        false if decoded == b"*" => "*".into(),
        false => crate::go::escape_path(&decoded),
    })
}
