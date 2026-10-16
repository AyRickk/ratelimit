//! `USE_PROMETHEUS`: upstream's Prometheus sink (`src/stats/prom/prometheus_sink.go`). Each
//! flushed stat goes through statsd_exporter's mapper and exporter into a registry served in
//! Prometheus' text format on `PROMETHEUS_ADDR` + `PROMETHEUS_PATH`.

mod exporter;
mod fsm;
mod mapper;

pub use mapper::{
    Action, HistogramOptions, Mapped, Mapper, MapperError, MatchType, Objective, ObserverType,
    SummaryOptions,
};

use bytes::Bytes;
use exporter::{Event, Exporter};
use http_body_util::Full;
use std::io::Write;
use std::sync::{Arc, Mutex};

/// The mapper upstream uses when `PROMETHEUS_MAPPER_YAML` is not set.
const DEFAULT_MAPPER: &str = include_str!("default_mapper.yaml");

/// The type of a stat, as the mapper's `match_metric_type` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Observer,
}

impl MetricType {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Observer => "observer",
        }
    }
}

/// The `PROMETHEUS_*` settings.
#[derive(Debug, Clone)]
pub struct PrometheusSettings {
    /// `PROMETHEUS_ADDR`.
    pub addr: String,
    /// `PROMETHEUS_PATH`.
    pub path: String,
    /// `PROMETHEUS_MAPPER_YAML`: a file, or empty for the default mapper.
    pub mapper_yaml: String,
    /// `PROMETHEUS_RESPONSE_TIME_AS_MILLISECONDS`.
    pub response_time_as_milliseconds: bool,
}

impl Default for PrometheusSettings {
    fn default() -> Self {
        Self {
            addr: ":9090".into(),
            path: "/metrics".into(),
            mapper_yaml: String::new(),
            response_time_as_milliseconds: false,
        }
    }
}

/// The mapper the settings select, as upstream loads it: a file that cannot be read or is
/// invalid leaves a mapper that maps nothing (the error is ignored, and the default mapper is
/// not used instead). Only a file that would make upstream panic is an error.
pub fn load_mapper(settings: &PrometheusSettings) -> Result<Mapper, String> {
    let content = if settings.mapper_yaml.is_empty() {
        match settings.response_time_as_milliseconds {
            true => DEFAULT_MAPPER.replacen("    scale: 0.001\n", "", 1),
            false => DEFAULT_MAPPER.to_string(),
        }
    } else {
        match std::fs::read(&settings.mapper_yaml) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
            Err(_) => return Ok(Mapper::default()),
        }
    };
    match Mapper::from_yaml(&content) {
        Ok(mapper) => Ok(mapper),
        Err(MapperError::Invalid(_)) => Ok(Mapper::default()),
        Err(MapperError::Panic(e)) => Err(format!("invalid PROMETHEUS_MAPPER_YAML: {e}")),
    }
}

/// The clock of the exporter: nanoseconds since the Unix epoch.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// The system clock.
pub fn system_clock() -> Clock {
    Arc::new(|| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(now.as_nanos()).unwrap_or(i64::MAX)
    })
}

/// The Prometheus sink. Clones share the registry.
#[derive(Clone)]
pub struct PrometheusSink {
    exporter: Arc<Mutex<Exporter>>,
    clock: Clock,
    path: MetricsPath,
}

impl std::fmt::Debug for PrometheusSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusSink").finish_non_exhaustive()
    }
}

impl PrometheusSink {
    /// A sink serving `path`, without its listener or its stale-series ticker.
    pub fn new(mapper: Mapper, path: &str, clock: Clock) -> Result<Self, String> {
        Ok(Self {
            exporter: Arc::new(Mutex::new(Exporter::new(mapper))),
            clock,
            path: MetricsPath::parse(path)?,
        })
    }

    /// `NewPrometheusSink`: loads the mapper, then serves on `PROMETHEUS_ADDR` (an address it
    /// cannot listen on is ignored, as upstream) and removes stale series every second.
    pub fn start(settings: &PrometheusSettings) -> Result<Self, String> {
        let path = match settings.path.as_str() {
            "" => "/metrics",
            path => path,
        };
        let sink = Self::new(load_mapper(settings)?, path, system_clock())?;
        let addr = match settings.addr.as_str() {
            "" => ":9090",
            addr => addr,
        };
        let address = listen_address(addr);
        let server = sink.clone();
        tokio::spawn(async move {
            if let Ok(listener) = tokio::net::TcpListener::bind(address).await {
                server.serve(listener).await;
            }
        });
        let ticker = sink.clone();
        tokio::spawn(async move {
            let second = std::time::Duration::from_secs(1);
            let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + second, second);
            loop {
                ticks.tick().await;
                ticker.remove_stale_metrics();
            }
        });
        Ok(sink)
    }

    fn handle(&self, name: &str, event: Event) {
        let now = (self.clock)();
        self.exporter.lock().unwrap().handle(name, event, now);
    }

    pub fn flush_counter(&self, name: &str, value: u64) {
        self.handle(name, Event::Counter(value as f64));
    }

    pub fn flush_gauge(&self, name: &str, value: u64) {
        self.handle(name, Event::Gauge(value as f64));
    }

    pub fn flush_timer(&self, name: &str, value: f64) {
        self.handle(name, Event::Observer(value));
    }

    /// Deletes the series whose TTL has passed (run every second by `start`).
    pub fn remove_stale_metrics(&self) {
        let now = (self.clock)();
        self.exporter.lock().unwrap().remove_stale_metrics(now);
    }

    /// Serves HTTP/1 requests from `listener`, as Go's `http.Serve` with the default mux.
    pub async fn serve(&self, listener: tokio::net::TcpListener) {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let sink = self.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request| {
                    let response = sink.respond(&request);
                    async move { Ok::<_, std::convert::Infallible>(response) }
                });
                let io = hyper_util::rt::TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            });
        }
    }

    fn respond<B>(&self, request: &hyper::Request<B>) -> hyper::Response<Full<Bytes>> {
        let header = |name| {
            request
                .headers()
                .get(name)
                .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
                .unwrap_or_default()
        };
        let uri = request.uri();
        let route = self.path.route(uri.path(), uri.query());
        let response = match route {
            Route::Metrics => self.metrics(&header("accept"), &header("accept-encoding")),
            Route::Redirect(location) => redirect(&location, request.method()),
            Route::NotFound => text_error(404, "404 page not found"),
        };
        let mut builder = hyper::Response::builder().status(response.status);
        for (name, value) in response.headers {
            builder = builder.header(name, value);
        }
        builder.body(Full::new(Bytes::from(response.body))).unwrap()
    }

    /// promhttp's handler for the default registry.
    fn metrics(&self, accept: &str, accept_encoding: &str) -> Response {
        let scraped = self.exporter.lock().unwrap().scrape();
        let text = match scraped {
            Ok(text) => text,
            Err(e) => {
                return text_error(
                    500,
                    &format!("An error has occurred while serving metrics:\n\n{e}"),
                );
            }
        };
        let mut headers = vec![("content-type", content_type(accept))];
        let body = if gzip_accepted(accept_encoding) {
            headers.push(("content-encoding", "gzip".into()));
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(text.as_bytes()).unwrap();
            encoder.finish().unwrap()
        } else {
            text.into_bytes()
        };
        Response {
            status: 200,
            headers,
            body,
        }
    }
}

/// Go's listen address: `:port` (or an empty host) listens on every interface.
fn listen_address(addr: &str) -> String {
    match addr.strip_prefix(':') {
        Some(port) => format!("[::]:{port}"),
        None => addr.to_string(),
    }
}

struct Response {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

/// `http.Error`.
fn text_error(status: u16, message: &str) -> Response {
    Response {
        status,
        headers: vec![
            ("content-type", "text/plain; charset=utf-8".into()),
            ("x-content-type-options", "nosniff".into()),
        ],
        body: format!("{message}\n").into_bytes(),
    }
}

/// `http.Redirect` with 307 (Go 1.27's mux), for a location that is already a clean absolute path.
fn redirect(location: &str, method: &hyper::Method) -> Response {
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
    Response {
        status: 307,
        headers,
        body,
    }
}

/// `gzipAccepted`: a `gzip` element in the first `Accept-Encoding` header, whatever its q.
fn gzip_accepted(accept_encoding: &str) -> bool {
    accept_encoding
        .split(',')
        .map(str::trim)
        .any(|part| part == "gzip" || part.starts_with("gzip;"))
}

/// expfmt's `Negotiate` without OpenMetrics: always the text format here (a protobuf request
/// gets it too, `docs/parity.md` §7), with the `escaping` parameter of the Accept clauses
/// read up to the one chosen.
fn content_type(accept: &str) -> String {
    let mut escaping = "values".to_string();
    for clause in parse_accept(accept) {
        if let Some(e) = clause.params.get("escaping")
            && matches!(
                e.as_str(),
                "allow-utf-8" | "underscores" | "dots" | "values"
            )
        {
            escaping.clone_from(e);
        }
        let version = clause.params.get("version").map_or("", String::as_str);
        let proto = clause.media == "application/vnd.google.protobuf"
            && clause.params.get("proto").map(String::as_str)
                == Some("io.prometheus.client.MetricFamily")
            && matches!(
                clause.params.get("encoding").map(String::as_str),
                Some("delimited" | "text" | "compact-text")
            );
        if proto || (clause.media == "text/plain" && matches!(version, "0.0.4" | "")) {
            break;
        }
    }
    format!("text/plain; version=0.0.4; charset=utf-8; escaping={escaping}")
}

struct AcceptClause {
    media: String,
    is_wild_type: bool,
    is_wild_subtype: bool,
    q: f64,
    params: std::collections::HashMap<String, String>,
}

/// goautoneg's `ParseAccept`, then its `sort.Sort` by q and specificity (Go's insertion sort
/// below 13 clauses, which keeps this order exact).
fn parse_accept(header: &str) -> Vec<AcceptClause> {
    let mut clauses = Vec::new();
    for part in header.split(',') {
        let part = part.trim_matches(' ');
        let mut pieces = part.split(';');
        let range = pieces.next().unwrap_or("");
        let types: Vec<&str> = range.split('/').collect();
        let main = types[0].trim_matches(' ');
        let sub = match types.len() {
            1 if main == "*" => "*",
            2 => types[1].trim_matches(' '),
            _ => continue,
        };
        let mut clause = AcceptClause {
            media: format!("{main}/{sub}"),
            is_wild_type: main == "*",
            is_wild_subtype: sub == "*",
            q: 1.0,
            params: Default::default(),
        };
        for param in pieces {
            let Some((token, value)) = param.split_once('=') else {
                continue;
            };
            let token = token.trim_matches(' ');
            if token == "q" {
                clause.q = value.parse::<f32>().map_or(0.0, f64::from);
            } else {
                clause
                    .params
                    .insert(token.to_string(), value.trim_matches(' ').to_string());
            }
        }
        clauses.push(clause);
    }
    let less = |a: &AcceptClause, b: &AcceptClause| {
        a.q > b.q
            || (!a.is_wild_type && b.is_wild_type)
            || (!a.is_wild_subtype && b.is_wild_subtype)
    };
    for i in 1..clauses.len() {
        let mut j = i;
        while j > 0 && less(&clauses[j], &clauses[j - 1]) {
            clauses.swap(j, j - 1);
            j -= 1;
        }
    }
    clauses
}

/// `PROMETHEUS_PATH` as a Go `ServeMux` pattern: an exact path, or a subtree when it ends
/// with `/`. Patterns with a method, a host or wildcards are not supported (§7).
#[derive(Debug, Clone)]
struct MetricsPath(String);

enum Route {
    Metrics,
    Redirect(String),
    NotFound,
}

impl MetricsPath {
    fn parse(path: &str) -> Result<Self, String> {
        let unsupported = !path.starts_with('/')
            || path.contains([' ', '\t', '{', '}'])
            || clean_path(path) != path;
        if unsupported {
            return Err(format!("unsupported PROMETHEUS_PATH {path:?}"));
        }
        Ok(Self(path.to_string()))
    }

    fn matches(&self, path: &str) -> bool {
        match self.0.ends_with('/') {
            true => path.starts_with(&self.0),
            false => path == self.0,
        }
    }

    /// Go's `ServeMux.findHandler` for this one pattern, on the escaped path: a missing
    /// trailing slash and an unclean path are redirected, other paths are not found.
    fn route(&self, request_path: &str, query: Option<&str>) -> Route {
        let with_query = |path: String| match query {
            Some(query) if !query.is_empty() => format!("{path}?{query}"),
            _ => path,
        };
        let path = clean_path(request_path);
        // Only a subtree pattern matches a path with a trailing slash exactly.
        if !path.ends_with('/') && self.0 == format!("{path}/") {
            return Route::Redirect(with_query(format!("{path}/")));
        }
        if path != request_path {
            return Route::Redirect(with_query(path));
        }
        if self.matches(&path) {
            Route::Metrics
        } else {
            Route::NotFound
        }
    }
}

/// Go's `cleanPath`: `path.Clean` of a rooted path, keeping a trailing slash.
fn clean_path(p: &str) -> String {
    if p.is_empty() {
        return "/".into();
    }
    let p = if p.starts_with('/') {
        p.to_string()
    } else {
        format!("/{p}")
    };
    let mut parts: Vec<&str> = Vec::new();
    for part in p.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    let mut clean = format!("/{}", parts.join("/"));
    if p.ends_with('/') && clean != "/" {
        clean.push('/');
    }
    clean
}
