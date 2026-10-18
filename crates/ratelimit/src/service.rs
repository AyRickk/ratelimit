//! The rate limit decision for a `ShouldRateLimit` request. A port of envoyproxy/ratelimit
//! `src/service/ratelimit.go`.

use crate::config::{ConfigError, LimitLookup, RateLimit, RateLimitConfig};
use crate::limiter::{Clock, reset_seconds};
use crate::stats::Stats;
use prost_types::{ListValue, Struct, Value, value::Kind};
use ratelimit_proto::envoy::config::core::v3::HeaderValue;
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_response::{Code, DescriptorStatus, rate_limit::Unit},
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, RwLock};

/// An error from the counting backend (upstream's `RedisError`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CacheError(pub String);

/// The counting backend (upstream's `RateLimitCache`).
pub trait RateLimitCache: Send + Sync + 'static {
    /// Counts the request against `limits`, one per descriptor (`None`: nothing to check),
    /// and returns one status per descriptor.
    fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> impl Future<Output = Result<Vec<DescriptorStatus>, CacheError>> + Send;
}

impl<T: RateLimitCache> RateLimitCache for Arc<T> {
    fn do_limit(
        &self,
        request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> impl Future<Output = Result<Vec<DescriptorStatus>, CacheError>> + Send {
        T::do_limit(self, request, limits)
    }
}

/// Settings read by the service.
#[derive(Debug, Clone, Default)]
pub struct ServiceSettings {
    /// `SHADOW_MODE`: never answer `OVER_LIMIT`.
    pub global_shadow_mode: bool,
    /// `QUOTA_MODE`: treat every descriptor as a quota descriptor.
    pub global_quota_mode: bool,
    /// `LIMIT_RESPONSE_HEADERS_ENABLED` and the `LIMIT_*_HEADER` names: describe the
    /// descriptor closest to its limit in headers added to the response.
    pub response_headers: Option<LimitHeaders>,
    /// `LIMIT_REQUEST_HEADERS_ENABLED` and the `LIMIT_REQUEST_*_HEADER` names: the same,
    /// added to the request.
    pub request_headers: Option<LimitHeaders>,
    /// `LIMIT_PER_UNIT_HEADERS_ENABLED`: `RateLimit-Limit-<Unit>s` and
    /// `RateLimit-Remaining-<Unit>s` response headers, one pair per unit.
    pub per_unit_headers: bool,
    /// `RESPONSE_DYNAMIC_METADATA`: describe the request in the response's dynamic metadata.
    pub response_dynamic_metadata: bool,
    /// `USE_CALENDAR_MONTH_RATE_LIMIT`: a month's reset is the next UTC calendar month.
    pub use_calendar_month: bool,
}

/// The names of the three headers describing the descriptor closest to its limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitHeaders {
    pub limit: String,
    pub remaining: String,
    pub reset: String,
}

impl Default for LimitHeaders {
    fn default() -> Self {
        Self {
            limit: "RateLimit-Limit".into(),
            remaining: "RateLimit-Remaining".into(),
            reset: "RateLimit-Reset".into(),
        }
    }
}

/// Why a request could not be answered. Messages are upstream's.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// An invalid request or no configuration (upstream's `serviceError`).
    #[error("{0}")]
    Service(String),
    /// A request upstream cannot handle (it panics); answered with `INVALID_ARGUMENT` (D1).
    #[error("{0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Cache(#[from] CacheError),
}

fn require(condition: bool, message: &str) -> Result<(), ServiceError> {
    if condition {
        Ok(())
    } else {
        Err(ServiceError::Service(message.to_string()))
    }
}

pub struct RateLimitService<C> {
    cache: C,
    /// Re-read for the reset headers, as upstream does.
    clock: Arc<dyn Clock>,
    stats: Arc<Stats>,
    config: RwLock<Option<Arc<RateLimitConfig>>>,
    settings: ServiceSettings,
}

impl<C: RateLimitCache> RateLimitService<C> {
    pub fn new(
        cache: C,
        clock: Arc<dyn Clock>,
        stats: Arc<Stats>,
        settings: ServiceSettings,
    ) -> Self {
        Self {
            cache,
            clock,
            stats,
            config: RwLock::new(None),
            settings,
        }
    }

    /// Replaces the configuration used by subsequent requests.
    pub fn set_config(&self, config: RateLimitConfig) {
        self.update_config(Ok(config));
    }

    /// `GetCurrentConfig`: the configuration in use, if one loaded.
    pub fn current_config(&self) -> Option<Arc<RateLimitConfig>> {
        self.config.read().unwrap().clone()
    }

    /// `SetConfig`: uses a configuration that loaded; one that did not leaves the current one
    /// in place.
    pub fn update_config(&self, update: Result<RateLimitConfig, ConfigError>) {
        let config = match update {
            Ok(config) => config,
            Err(e) => {
                self.stats.config_load_error().inc();
                crate::log::error(&format!("Error loading new configuration: {e}"));
                return;
            }
        };
        self.stats.config_load_success().inc();
        *self.config.write().unwrap() = Some(Arc::new(config));
        crate::log::info("Successfully loaded new configuration");
    }

    pub async fn should_rate_limit(
        &self,
        request: &RateLimitRequest,
    ) -> Result<RateLimitResponse, ServiceError> {
        crate::debug!("ShouldRateLimit: {}", crate::prototext::request(request));
        // Its context is not passed on: the request's other spans are its siblings.
        let _span = crate::trace::start(
            crate::trace::SERVICE,
            "ShouldRateLimit Execution",
            crate::trace::Kind::Internal,
            crate::trace::parent().as_ref(),
            || {
                vec![
                    (
                        "domain".into(),
                        crate::trace::Value::Str(request.domain.clone()),
                    ),
                    (
                        "request string".into(),
                        crate::trace::Value::Str(crate::prototext::request(request)),
                    ),
                ]
            },
        );
        let response = self.decide(request).await;
        match &response {
            Ok(response) => crate::debug!(
                "returning normal response: {}",
                crate::prototext::response(response)
            ),
            Err(error) => crate::debug!("caught error during call: {error}"),
        }
        match &response {
            Err(ServiceError::Service(_)) => self.stats.service_error().inc(),
            Err(ServiceError::Cache(_)) => self.stats.redis_error().inc(),
            // Upstream panics on these (D1): nothing is counted.
            Ok(_) | Err(ServiceError::InvalidArgument(_)) => {}
        }
        response
    }

    async fn decide(&self, request: &RateLimitRequest) -> Result<RateLimitResponse, ServiceError> {
        require(
            !request.domain.is_empty(),
            "rate limit domain must not be empty",
        )?;
        require(
            !request.descriptors.is_empty(),
            "rate limit descriptor list must not be empty",
        )?;
        let config = self.current_config();
        let config = config
            .ok_or_else(|| ServiceError::Service("no rate limit configuration loaded".into()))?;
        let checks = descriptor_checks(&config, request, &self.stats);
        let unknown_unit = checks.iter().any(|check| {
            matches!(check, Check::Limit(Some(limit)) if limit.limit.unit() == Unit::Unknown)
        });
        if unknown_unit {
            return Err(ServiceError::InvalidArgument(
                "invalid rate limit unit UNKNOWN".into(),
            ));
        }
        let limits: Vec<Option<RateLimit>> = checks
            .iter()
            .map(|check| match check {
                Check::Limit(limit) => limit.clone(),
                Check::Unlimited => None,
            })
            .collect();
        let mut statuses = self.cache.do_limit(request, &limits).await?;
        crate::debug!(
            "descriptor statuses: {}",
            crate::prototext::statuses(&statuses)
        );
        if statuses.len() != checks.len() {
            // Upstream asserts here and crashes; this is a backend bug, reported as such.
            return Err(CacheError(format!(
                "cache returned {} statuses for {} descriptors",
                statuses.len(),
                checks.len()
            ))
            .into());
        }

        // A regular descriptor over its limit makes the request over. Quota descriptors are
        // grouped by model: a group is over when any of its descriptors is, and the request
        // only when every group is (so another model can take over).
        let mut regular_over = false;
        let mut quota_groups: HashMap<String, bool> = HashMap::new();
        // The descriptor closest to its limit, for the headers: chosen among the backend's
        // statuses (unlimited ones have no limit), the first on a tie, but the last regular
        // descriptor over its limit wins outright, counting as 0 remaining.
        let mut closest: Option<DescriptorStatus> = None;
        let mut min_remaining = u32::MAX;
        // The descriptors that passed (unlimited ones excepted), for the metadata.
        let mut passed: Vec<Passed> = Vec::new();
        for ((status, check), descriptor) in
            statuses.iter_mut().zip(&checks).zip(&request.descriptors)
        {
            if status.current_limit.is_some() && status.limit_remaining < min_remaining {
                closest = Some(status.clone());
                min_remaining = status.limit_remaining;
            }
            let limit = match check {
                Check::Unlimited => {
                    *status = DescriptorStatus {
                        code: Code::Ok as i32,
                        limit_remaining: u32::MAX,
                        ..Default::default()
                    };
                    continue;
                }
                Check::Limit(limit) => limit,
            };
            let over = status.code() == Code::OverLimit;
            let quota_mode =
                self.settings.global_quota_mode || limit.as_ref().is_some_and(|l| l.quota_mode);
            let quota_group = quota_mode.then(|| quota_group_key(descriptor));
            if let Some(group) = &quota_group {
                *quota_groups.entry(group.clone()).or_default() |= over;
            }
            if !over {
                passed.push(Passed {
                    descriptor,
                    limit: limit.as_ref(),
                    quota_group,
                });
            } else if !quota_mode {
                regular_over = true;
                closest = Some(status.clone());
                min_remaining = 0;
            }
        }
        let quota_over = !quota_groups.is_empty() && quota_groups.values().all(|over| *over);
        let mut overall_code = if regular_over || quota_over {
            Code::OverLimit
        } else {
            Code::Ok
        };
        let mut response_headers_to_add = Vec::new();
        if self.settings.per_unit_headers {
            response_headers_to_add = per_unit_headers(&statuses);
        }
        if let (Some(names), Some(closest)) = (&self.settings.response_headers, &closest) {
            response_headers_to_add.extend(self.limit_headers(names, closest)?);
        }
        let mut request_headers_to_add = Vec::new();
        if let (Some(names), Some(closest)) = (&self.settings.request_headers, &closest) {
            request_headers_to_add = self.limit_headers(names, closest)?;
        }
        if overall_code == Code::OverLimit && self.settings.global_shadow_mode {
            overall_code = Code::Ok;
            self.stats.global_shadow_mode().inc();
        }
        let dynamic_metadata = self.settings.response_dynamic_metadata.then(|| {
            // A descriptor that passed is not available when its quota group is exhausted.
            passed.retain(|p| {
                p.quota_group
                    .as_ref()
                    .is_none_or(|group| !quota_groups[group])
            });
            dynamic_metadata(request, &passed, !quota_groups.is_empty())
        });
        Ok(RateLimitResponse {
            overall_code: overall_code as i32,
            statuses,
            response_headers_to_add,
            request_headers_to_add,
            dynamic_metadata,
            ..Default::default()
        })
    }

    /// The limit, remaining hits and seconds until reset of `status`, under `names`. The reset
    /// re-reads the clock rather than using the status's `duration_until_reset`, as upstream.
    fn limit_headers(
        &self,
        names: &LimitHeaders,
        status: &DescriptorStatus,
    ) -> Result<Vec<HeaderValue>, ServiceError> {
        let limit = status
            .current_limit
            .as_ref()
            .expect("a status with a limit");
        let now = self.clock.unix_now();
        let reset = reset_seconds(limit.unit(), now, self.settings.use_calendar_month)?;
        Ok(vec![
            header(&names.limit, limit.requests_per_unit),
            header(&names.remaining, status.limit_remaining),
            header(&names.reset, reset),
        ])
    }
}

/// `RateLimit-Limit-<Unit>s` and `RateLimit-Remaining-<Unit>s` for each unit, in the order
/// units first appear, describing the descriptor of that unit with the fewest remaining hits
/// (the first on a tie). Descriptors without a limit (unlimited ones included) are skipped.
fn per_unit_headers(statuses: &[DescriptorStatus]) -> Vec<HeaderValue> {
    let mut closest_by_unit: Vec<(Unit, &DescriptorStatus)> = Vec::new();
    for status in statuses {
        let Some(limit) = &status.current_limit else {
            continue;
        };
        match closest_by_unit
            .iter_mut()
            .find(|(unit, _)| *unit == limit.unit())
        {
            None => closest_by_unit.push((limit.unit(), status)),
            Some((_, closest)) if status.limit_remaining < closest.limit_remaining => {
                *closest = status;
            }
            Some(_) => {}
        }
    }
    closest_by_unit
        .into_iter()
        .flat_map(|(unit, status)| {
            let name = unit.as_str_name();
            let suffix = format!("{}{}s", &name[..1], name[1..].to_lowercase());
            let limit = status.current_limit.as_ref().unwrap().requests_per_unit;
            [
                header(&format!("RateLimit-Limit-{suffix}"), limit),
                header(
                    &format!("RateLimit-Remaining-{suffix}"),
                    status.limit_remaining,
                ),
            ]
        })
        .collect()
}

/// A descriptor that passed.
struct Passed<'a> {
    descriptor: &'a RateLimitDescriptor,
    limit: Option<&'a RateLimit>,
    /// Its quota group, in quota mode.
    quota_group: Option<String>,
}

/// `ratelimitToMetadata`: the request's `domain`, its `descriptors`, its `hitsAddend` when not
/// 0, the `metadata` of the `available` limits merged and, in `quota_mode`, the models
/// (`passedBackends`) of the `available` descriptors.
fn dynamic_metadata(request: &RateLimitRequest, available: &[Passed], quota_mode: bool) -> Struct {
    let mut fields = BTreeMap::new();
    fields.insert("domain".into(), string(&request.domain));
    let descriptors = request
        .descriptors
        .iter()
        .map(descriptor_metadata)
        .collect();
    fields.insert("descriptors".into(), list(descriptors));
    if request.hits_addend != 0 {
        fields.insert("hitsAddend".into(), number(request.hits_addend as f64));
    }
    let mut metadata = Struct::default();
    for limit_metadata in available.iter().filter_map(|p| p.limit?.metadata.as_ref()) {
        merge_metadata(&mut metadata, limit_metadata);
    }
    if !metadata.fields.is_empty() {
        fields.insert("metadata".into(), struct_value(metadata));
    }
    if quota_mode {
        let backends = passed_backends(available);
        if !backends.is_empty() {
            fields.insert("passedBackends".into(), list(backends));
        }
    }
    Struct { fields }
}

/// `passedBackendsList`: the `backend_name` and `model_name_override` of each descriptor naming
/// a model, once per quota group, whether the descriptor is a quota one or not.
fn passed_backends(available: &[Passed]) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut backends = Vec::new();
    for passed in available {
        let (backend, model) = backend_and_model(passed.descriptor);
        if (backend.is_empty() && model.is_empty())
            || !seen.insert(quota_group_key(passed.descriptor))
        {
            continue;
        }
        let fields = BTreeMap::from([
            ("backend_name".to_string(), string(backend)),
            ("model_name_override".to_string(), string(model)),
        ]);
        backends.push(struct_value(Struct { fields }));
    }
    backends
}

/// `mergeMetadata`: adds the fields of `source` missing from `destination`, merging structs
/// present in both. Upstream merges into the configuration's own structs, which then keep the
/// merged fields for later requests; this merges into copies (docs/parity.md section 7).
fn merge_metadata(destination: &mut Struct, source: &Struct) {
    for (key, value) in &source.fields {
        match destination.fields.get_mut(key) {
            None => {
                destination.fields.insert(key.clone(), value.clone());
            }
            Some(Value {
                kind: Some(Kind::StructValue(existing)),
            }) => {
                if let Some(Kind::StructValue(source)) = &value.kind {
                    merge_metadata(existing, source);
                }
            }
            Some(_) => {}
        }
    }
}

/// `descriptorToStruct`: the `entries` as `key=value`, and the `hitsAddend` when set, even to 0.
fn descriptor_metadata(descriptor: &RateLimitDescriptor) -> Value {
    let mut fields = BTreeMap::new();
    let entries = descriptor
        .entries
        .iter()
        .map(|entry| string(&format!("{}={}", entry.key, entry.value)))
        .collect();
    fields.insert("entries".into(), list(entries));
    if let Some(limit) = &descriptor.limit {
        fields.insert("limit".into(), string(&crate::prototext::limit(limit)));
    }
    if let Some(hits) = descriptor.hits_addend {
        fields.insert("hitsAddend".into(), number(hits as f64));
    }
    struct_value(Struct { fields })
}

fn struct_value(value: Struct) -> Value {
    Value {
        kind: Some(Kind::StructValue(value)),
    }
}

fn string(value: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(value.to_string())),
    }
}

fn number(value: f64) -> Value {
    Value {
        kind: Some(Kind::NumberValue(value)),
    }
}

fn list(values: Vec<Value>) -> Value {
    Value {
        kind: Some(Kind::ListValue(ListValue { values })),
    }
}

fn header(key: &str, value: impl ToString) -> HeaderValue {
    HeaderValue {
        key: key.to_string(),
        value: value.to_string(),
        ..Default::default()
    }
}

/// What to do with one descriptor of a request.
enum Check {
    /// Its limit is `unlimited`: never counted, always `OK`.
    Unlimited,
    /// Count it against this limit, if any.
    Limit(Option<RateLimit>),
}

/// `constructLimitsToCheck`: each descriptor's limit, left out when another descriptor's
/// limit replaces it by name (`replaces`). Replacements are collected from every matched
/// limit first, unlimited and replaced ones included, so they are not transitive.
fn descriptor_checks(
    config: &RateLimitConfig,
    request: &RateLimitRequest,
    stats: &Stats,
) -> Vec<Check> {
    let mut replaced = HashSet::new();
    let mut checks: Vec<Check> = request
        .descriptors
        .iter()
        .map(|descriptor| {
            crate::debug!(
                "got descriptor: {}",
                descriptor
                    .entries
                    .iter()
                    .map(|e| format!("({}={})", e.key, e.value))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let lookup = config.get_limit(&request.domain, descriptor);
            match &lookup {
                LimitLookup::Limit(limit) if limit.unlimited => {
                    crate::debug!("descriptor is unlimited, not passing to the cache")
                }
                LimitLookup::Limit(limit) => crate::debug!(
                    "applying limit: {} requests per {}, shadow_mode: {}, quota: {}",
                    limit.limit.requests_per_unit,
                    crate::config::unit_name(limit.limit.unit),
                    limit.shadow_mode,
                    limit.quota_mode
                ),
                _ => crate::debug!("descriptor does not match any limit, no limits applied"),
            }
            match lookup {
                LimitLookup::Limit(limit) => {
                    replaced.extend(limit.replaces.iter().cloned());
                    if limit.unlimited {
                        Check::Unlimited
                    } else {
                        Check::Limit(Some(limit))
                    }
                }
                LimitLookup::UnknownDomain => {
                    // Counted per descriptor, as upstream counts each lookup.
                    stats.domain_not_found(&request.domain).inc();
                    Check::Limit(None)
                }
                LimitLookup::NoLimit => Check::Limit(None),
            }
        })
        .collect();
    for check in &mut checks {
        if let Check::Limit(limit) = check
            && limit
                .as_ref()
                .is_some_and(|l| !l.name.is_empty() && replaced.contains(&l.name))
        {
            crate::debug!("replacing {}", limit.as_ref().unwrap().name);
            *limit = None;
        }
    }
    checks
}

/// The quota group of a descriptor: its model, from the `backend_name` and
/// `model_name_override` entries, or else all of its entries.
fn quota_group_key(descriptor: &RateLimitDescriptor) -> String {
    let (backend, model) = backend_and_model(descriptor);
    if backend.is_empty() && model.is_empty() {
        descriptor
            .entries
            .iter()
            .map(|entry| format!("{}={};", entry.key, entry.value))
            .collect()
    } else {
        format!("{backend}|{model}")
    }
}

/// The model a descriptor names: its last `backend_name` and `model_name_override` entries.
fn backend_and_model(descriptor: &RateLimitDescriptor) -> (&str, &str) {
    let mut backend = "";
    let mut model = "";
    for entry in &descriptor.entries {
        match entry.key.as_str() {
            "backend_name" => backend = &entry.value,
            "model_name_override" => model = &entry.value,
            _ => {}
        }
    }
    (backend, model)
}
