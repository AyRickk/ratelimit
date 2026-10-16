//! The rate limit decision for a `ShouldRateLimit` request. A port of envoyproxy/ratelimit
//! `src/service/ratelimit.go`.

use crate::config::{LimitLookup, RateLimit, RateLimitConfig};
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_response::{Code, DescriptorStatus},
};
use std::collections::HashMap;
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
}

/// Why a request could not be answered. Messages are upstream's.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceError {
    /// An invalid request or no configuration (upstream's `serviceError`).
    #[error("{0}")]
    Service(String),
    #[error(transparent)]
    Cache(#[from] CacheError),
}

fn check(condition: bool, message: &str) -> Result<(), ServiceError> {
    if condition {
        Ok(())
    } else {
        Err(ServiceError::Service(message.to_string()))
    }
}

pub struct RateLimitService<C> {
    cache: C,
    config: RwLock<Option<Arc<RateLimitConfig>>>,
    settings: ServiceSettings,
}

impl<C: RateLimitCache> RateLimitService<C> {
    pub fn new(cache: C, settings: ServiceSettings) -> Self {
        Self {
            cache,
            config: RwLock::new(None),
            settings,
        }
    }

    /// Replaces the configuration used by subsequent requests.
    pub fn set_config(&self, config: RateLimitConfig) {
        *self.config.write().unwrap() = Some(Arc::new(config));
    }

    pub async fn should_rate_limit(
        &self,
        request: &RateLimitRequest,
    ) -> Result<RateLimitResponse, ServiceError> {
        check(
            !request.domain.is_empty(),
            "rate limit domain must not be empty",
        )?;
        check(
            !request.descriptors.is_empty(),
            "rate limit descriptor list must not be empty",
        )?;
        let config = self.config.read().unwrap().clone();
        let config = config
            .ok_or_else(|| ServiceError::Service("no rate limit configuration loaded".into()))?;
        let (limits, unlimited) = limits_to_check(&config, request);
        let mut statuses = self.cache.do_limit(request, &limits).await?;

        // A regular descriptor over its limit makes the request over. Quota descriptors are
        // grouped by model: a group is over when any of its descriptors is, and the request
        // only when every group is (so another model can take over).
        let mut regular_over = false;
        let mut quota_groups: HashMap<String, bool> = HashMap::new();
        for (i, status) in statuses.iter_mut().enumerate() {
            if unlimited[i] {
                *status = DescriptorStatus {
                    code: Code::Ok as i32,
                    limit_remaining: u32::MAX,
                    ..Default::default()
                };
                continue;
            }
            let over = status.code() == Code::OverLimit;
            let quota_mode =
                self.settings.global_quota_mode || limits[i].as_ref().is_some_and(|l| l.quota_mode);
            if quota_mode {
                *quota_groups
                    .entry(quota_group_key(&request.descriptors[i]))
                    .or_default() |= over;
            } else if over {
                regular_over = true;
            }
        }
        let quota_over = !quota_groups.is_empty() && quota_groups.values().all(|over| *over);
        let mut overall_code = if regular_over || quota_over {
            Code::OverLimit
        } else {
            Code::Ok
        };
        if overall_code == Code::OverLimit && self.settings.global_shadow_mode {
            overall_code = Code::Ok;
        }
        Ok(RateLimitResponse {
            overall_code: overall_code as i32,
            statuses,
            ..Default::default()
        })
    }
}

/// `constructLimitsToCheck`: the limit of each descriptor, with unlimited ones and those
/// replaced by another descriptor's limit (`replaces`, by name) left out, and which
/// descriptors are unlimited.
fn limits_to_check(
    config: &RateLimitConfig,
    request: &RateLimitRequest,
) -> (Vec<Option<RateLimit>>, Vec<bool>) {
    let mut limits = Vec::with_capacity(request.descriptors.len());
    let mut unlimited = Vec::with_capacity(request.descriptors.len());
    let mut replaced = std::collections::HashSet::new();
    for descriptor in &request.descriptors {
        let limit = match config.get_limit(&request.domain, descriptor) {
            LimitLookup::Limit(limit) => Some(limit),
            LimitLookup::NoLimit | LimitLookup::UnknownDomain => None,
        };
        if let Some(limit) = &limit {
            replaced.extend(limit.replaces.iter().cloned());
        }
        let is_unlimited = limit.as_ref().is_some_and(|l| l.unlimited);
        unlimited.push(is_unlimited);
        limits.push(limit.filter(|_| !is_unlimited));
    }
    for limit in &mut limits {
        if limit
            .as_ref()
            .is_some_and(|l| !l.name.is_empty() && replaced.contains(&l.name))
        {
            *limit = None;
        }
    }
    (limits, unlimited)
}

/// The quota group of a descriptor: its model, from the `backend_name` and
/// `model_name_override` entries, or else all of its entries.
fn quota_group_key(descriptor: &RateLimitDescriptor) -> String {
    let mut backend = "";
    let mut model = "";
    let mut all = String::new();
    for entry in &descriptor.entries {
        match entry.key.as_str() {
            "backend_name" => backend = &entry.value,
            "model_name_override" => model = &entry.value,
            _ => {}
        }
        all += &format!("{}={};", entry.key, entry.value);
    }
    if backend.is_empty() && model.is_empty() {
        all
    } else {
        format!("{backend}|{model}")
    }
}
