//! Upstream's OpenTelemetry tracing (`src/trace/trace.go` and the spans of the service,
//! the Redis cache and `/json`): with `TRACING_ENABLED`, spans sampled by
//! `ParentBased(TraceIDRatioBased(TRACING_SAMPLING_RATE))`, batched and exported over OTLP
//! (gRPC or HTTP) to where the `OTEL_EXPORTER_OTLP_*` variables say. The resource has only
//! `service.name`, `service.namespace` and `service.instance.id`, with
//! `OTEL_RESOURCE_ATTRIBUTES`.

mod export;
pub mod grpc;
pub mod propagation;

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use export::Protocol;

/// A span's identity, as it is propagated: the parent of the spans started under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    /// W3C trace flags: sampled (`0x01`) and random (`0x02`).
    pub flags: u8,
    /// The W3C `tracestate`, as `TraceState.String()` writes it.
    pub trace_state: String,
    /// Extracted from a request rather than started here.
    pub remote: bool,
}

impl SpanContext {
    pub fn sampled(&self) -> bool {
        self.flags & 1 == 1
    }
}

/// An instrumentation scope: the tracer a span is started with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scope {
    pub name: &'static str,
    pub version: &'static str,
}

/// The service's spans (`ShouldRateLimit Execution`).
pub const SERVICE: Scope = Scope {
    name: "ratelimit",
    version: "",
};
/// The Redis cache's spans (`Redis Pipeline Execution`).
pub const REDIS: Scope = Scope {
    name: "redis.fixedCacheImpl",
    version: "",
};
/// `/json`'s span (`NewJsonHandler Remaining Execution`).
pub const SERVER: Scope = Scope {
    name: "ratelimit server",
    version: "",
};

/// A span kind, numbered as OTLP's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Internal = 1,
    Server = 2,
}

/// An attribute's value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
}

/// The settings `InitProductionTraceProvider` takes.
#[derive(Debug, Clone)]
pub struct Settings {
    pub protocol: String,
    pub service_name: String,
    pub service_namespace: String,
    pub service_instance_id: String,
    pub sampling_rate: f64,
}

/// What was started: the sampler, the resource and the batch queue.
struct Tracer {
    /// `TraceIDRatioBased`: a root span is sampled when the low 63 bits of its trace ID are
    /// below this bound (`u64::MAX` always, 0 never).
    bound: u64,
    queue: export::Queue,
}

static TRACER: OnceLock<Tracer> = OnceLock::new();

/// `InitProductionTraceProvider`: an unknown protocol is fatal; with no instance ID, a
/// random UUID. Starts the exporter and logs the parameters.
pub fn init(settings: &Settings) -> Result<(), crate::log::Failure> {
    let protocol = match settings.protocol.as_str() {
        "http" | "" => Protocol::Http,
        "grpc" => Protocol::Grpc,
        other => {
            return Err(crate::log::Failure::Fatal(format!(
                "Invalid otlptrace client protocol: {other}"
            )));
        }
    };
    let instance_id = match settings.service_instance_id.as_str() {
        "" => random_uuid(),
        id => id.to_string(),
    };
    let resource = export::resource(
        &settings.service_name,
        &settings.service_namespace,
        &instance_id,
    );
    let rate = settings.sampling_rate;
    let bound = ratio_bound(rate);
    let queue = export::start(protocol, resource);
    let _ = TRACER.set(Tracer { bound, queue });
    crate::log::info(&format!(
        "TracerProvider initialized with following parameters: protocol: {}, serviceName: {}, serviceNamespace: {}, serviceInstanceId: {instance_id}, samplingRate: {}",
        settings.protocol,
        settings.service_name,
        settings.service_namespace,
        crate::go::format_fixed(rate, 6)
    ));
    Ok(())
}

/// `tp.Shutdown` within 5 seconds: the spans still queued are exported.
pub async fn shutdown() {
    let Some(tracer) = TRACER.get() else { return };
    let flushed = tokio::time::timeout(Duration::from_secs(5), tracer.queue.shutdown()).await;
    let error = match flushed {
        Ok(Ok(())) => return,
        Ok(Err(e)) => e,
        Err(_) => "context deadline exceeded".to_string(),
    };
    crate::log::info(&format!("Error shutting down tracer provider: {error}"));
}

/// Whether spans are recorded at all.
pub fn enabled() -> bool {
    TRACER.get().is_some()
}

/// A started span, ended when dropped (as Go's `defer span.End()`).
pub struct Span {
    context: Option<SpanContext>,
    recording: Option<export::SpanData>,
}

impl Span {
    /// The span's context, for the spans started under it; none when tracing is off.
    pub fn context(&self) -> Option<&SpanContext> {
        self.context.as_ref()
    }

    /// Whether the span is sampled, so recorded and exported.
    pub fn is_recording(&self) -> bool {
        self.recording.is_some()
    }

    /// Sets an attribute, replacing one with the same key.
    pub fn set_attribute(&mut self, key: &str, value: Value) {
        if let Some(span) = &mut self.recording {
            match span.attributes.iter_mut().find(|(k, _)| k == key) {
                Some(existing) => existing.1 = value,
                None => span.attributes.push((key.to_string(), value)),
            }
        }
    }

    /// The span's status: `Error` with a message.
    pub fn set_error(&mut self, message: &str) {
        if let Some(span) = &mut self.recording {
            span.status = Some(message.to_string());
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let (Some(mut span), Some(tracer)) = (self.recording.take(), TRACER.get()) {
            span.end = now();
            tracer.queue.push(span);
        }
    }
}

/// Starts a span under `parent` (a new trace without one), with `attributes` (built only if
/// it is sampled). Sampled as `ParentBased(TraceIDRatioBased)`: as its parent, or by its trace
/// ID for a root.
pub fn start(
    scope: Scope,
    name: &str,
    kind: Kind,
    parent: Option<&SpanContext>,
    attributes: impl FnOnce() -> Vec<(String, Value)>,
) -> Span {
    let Some(tracer) = TRACER.get() else {
        return Span {
            context: None,
            recording: None,
        };
    };
    let span_id = random_span_id();
    let (trace_id, sampled) = match parent {
        Some(parent) => (parent.trace_id, parent.sampled()),
        None => {
            let trace_id = random_trace_id();
            let low = u64::from_be_bytes(trace_id[8..].try_into().unwrap()) >> 1;
            (trace_id, tracer.bound == u64::MAX || low < tracer.bound)
        }
    };
    let parent_flags = parent.map_or(0, |p| p.flags);
    let context = SpanContext {
        trace_id,
        span_id,
        flags: if sampled {
            parent_flags | 1
        } else {
            parent_flags & !1
        },
        trace_state: parent.map(|p| p.trace_state.clone()).unwrap_or_default(),
        remote: false,
    };
    let recording = sampled.then(|| export::SpanData {
        scope,
        context: context.clone(),
        parent_span_id: parent.map(|p| p.span_id),
        // otelgrpc marks what it extracted as remote, even nothing: a server span's parent
        // is always remote.
        parent_remote: kind == Kind::Server || parent.is_some_and(|p| p.remote),
        name: name.to_string(),
        kind,
        start: now(),
        end: 0,
        attributes: attributes(),
        status: None,
    });
    Span {
        context: Some(context),
        recording,
    }
}

tokio::task_local! {
    /// The span a request's own spans are started under: the gRPC server span (Go passes it
    /// in the request's context).
    static PARENT: Option<SpanContext>;
}

/// Runs `future` with `parent` as the parent of the spans it starts.
pub async fn with_parent<F: std::future::Future>(
    parent: Option<SpanContext>,
    future: F,
) -> F::Output {
    PARENT.scope(parent, future).await
}

/// The parent set by [`with_parent`], if any.
pub fn parent() -> Option<SpanContext> {
    PARENT.try_with(|p| p.clone()).ok().flatten()
}

/// `TraceIDRatioBased`'s bound: `u64::MAX` (always) from 1, 0 (never) up to 0.
fn ratio_bound(rate: f64) -> u64 {
    if rate >= 1.0 {
        u64::MAX
    } else if rate > 0.0 {
        (rate * (1u64 << 63) as f64) as u64
    } else {
        0
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// Random bytes from a per-thread generator (splitmix64), seeded from the system.
fn random_bytes(out: &mut [u8]) {
    thread_local! {
        static STATE: std::cell::Cell<u64> = std::cell::Cell::new({
            let mut seed = [0u8; 8];
            let _ = getrandom::fill(&mut seed);
            u64::from_ne_bytes(seed)
        });
    }
    for chunk in out.chunks_mut(8) {
        let value = STATE.with(|state| {
            let next = state.get().wrapping_add(0x9e37_79b9_7f4a_7c15);
            state.set(next);
            let mut z = next;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        });
        chunk.copy_from_slice(&value.to_be_bytes()[..chunk.len()]);
    }
}

fn random_trace_id() -> [u8; 16] {
    loop {
        let mut id = [0u8; 16];
        random_bytes(&mut id);
        if id != [0; 16] {
            return id;
        }
    }
}

fn random_span_id() -> [u8; 8] {
    loop {
        let mut id = [0u8; 8];
        random_bytes(&mut id);
        if id != [0; 8] {
            return id;
        }
    }
}

/// `uuid.NewRandom().String()`: a version 4 UUID.
fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratios_bound_trace_ids_like_go() {
        // `uint64(fraction * (1 << 63))`.
        assert_eq!(ratio_bound(0.5), 1 << 62);
        assert_eq!(ratio_bound(0.25), 1 << 61);
        assert_eq!(ratio_bound(1e-9), 9_223_372_036);
        assert_eq!(ratio_bound(1.0), u64::MAX);
        assert_eq!(ratio_bound(-1.0), 0);
    }

    #[test]
    fn instance_ids_are_version_4_uuids() {
        let id = random_uuid();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "4");
        assert!("89ab".contains(&id[19..20]), "{id}");
    }
}
