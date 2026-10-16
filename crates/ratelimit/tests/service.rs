//! The rate limit service's decision logic, ported from envoyproxy/ratelimit@f4e7669
//! `test/service/ratelimit_test.go`. The counting backend is a fake standing in for Redis.

use ratelimit::config::{self, ConfigFile, LoadOptions, RateLimit};
use ratelimit::service::{
    CacheError, RateLimitCache, RateLimitService, ServiceError, ServiceSettings,
};
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::Entry,
};
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_response::{Code, DescriptorStatus},
};
use std::sync::{Arc, Mutex};

/// A counting backend that returns preset statuses and records the limits it was asked to check.
#[derive(Default)]
struct FakeCache {
    statuses: Mutex<Vec<DescriptorStatus>>,
    received: Mutex<Vec<Vec<Option<RateLimit>>>>,
    error: Option<String>,
}

impl FakeCache {
    fn returning(statuses: Vec<DescriptorStatus>) -> Self {
        Self {
            statuses: Mutex::new(statuses),
            ..Default::default()
        }
    }

    fn received(&self) -> Vec<Vec<Option<RateLimit>>> {
        self.received.lock().unwrap().clone()
    }
}

impl RateLimitCache for FakeCache {
    async fn do_limit(
        &self,
        _request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> Result<Vec<DescriptorStatus>, CacheError> {
        self.received.lock().unwrap().push(limits.to_vec());
        if let Some(error) = &self.error {
            return Err(CacheError(error.clone()));
        }
        Ok(self.statuses.lock().unwrap().clone())
    }
}

fn load(yaml: &str) -> config::RateLimitConfig {
    let file = ConfigFile {
        name: "inline.yaml".into(),
        content: yaml.into(),
    };
    config::load(&[file], &LoadOptions::default()).unwrap()
}

fn request(domain: &str, descriptors: &[&[(&str, &str)]]) -> RateLimitRequest {
    RateLimitRequest {
        domain: domain.into(),
        descriptors: descriptors
            .iter()
            .map(|entries| RateLimitDescriptor {
                entries: entries
                    .iter()
                    .map(|(key, value)| Entry {
                        key: key.to_string(),
                        value: value.to_string(),
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        hits_addend: 1,
    }
}

fn status(code: Code, limit_remaining: u32) -> DescriptorStatus {
    DescriptorStatus {
        code: code as i32,
        limit_remaining,
        ..Default::default()
    }
}

const CONFIG: &str = "
domain: test-domain
descriptors:
  - key: foo
    rate_limit: {unit: minute, requests_per_unit: 10, name: key_name}
";

#[tokio::test]
async fn unmatched_descriptor_is_passed_to_the_cache_without_a_limit() {
    let cache = Arc::new(FakeCache::returning(vec![status(Code::Ok, 0)]));
    let service = RateLimitService::new(cache.clone(), ServiceSettings::default());
    service.set_config(load(CONFIG));

    let response = service
        .should_rate_limit(&request("test-domain", &[&[("hello", "world")]]))
        .await
        .unwrap();

    assert_eq!(
        response,
        RateLimitResponse {
            overall_code: Code::Ok as i32,
            statuses: vec![status(Code::Ok, 0)],
            ..Default::default()
        }
    );
    assert_eq!(cache.received(), [vec![None]]);
}

#[tokio::test]
async fn any_descriptor_over_limit_makes_the_request_over_limit() {
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::OverLimit, 0),
        status(Code::Ok, 0),
    ]));
    let service = RateLimitService::new(cache.clone(), ServiceSettings::default());
    service.set_config(load(CONFIG));

    let response = service
        .should_rate_limit(&request(
            "test-domain",
            &[&[("foo", "bar")], &[("hello", "world")]],
        ))
        .await
        .unwrap();

    assert_eq!(response.overall_code(), Code::OverLimit);
    assert_eq!(
        response.statuses,
        [status(Code::OverLimit, 0), status(Code::Ok, 0)]
    );
    let received = cache.received();
    let [limits] = received.as_slice() else {
        panic!("one cache call")
    };
    assert_eq!(
        limits[0]
            .as_ref()
            .map(|l| (l.name.as_str(), l.limit.requests_per_unit)),
        Some(("key_name", 10))
    );
    assert!(limits[1].is_none());
}

#[tokio::test]
async fn invalid_requests_and_missing_config_are_service_errors() {
    let service = RateLimitService::new(Arc::new(FakeCache::default()), ServiceSettings::default());

    let error = service
        .should_rate_limit(&request("test-domain", &[&[("hello", "world")]]))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ServiceError::Service("no rate limit configuration loaded".into())
    );

    service.set_config(load(CONFIG));
    let error = service
        .should_rate_limit(&request("", &[&[("hello", "world")]]))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "rate limit domain must not be empty");
    let error = service
        .should_rate_limit(&request("test-domain", &[]))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "rate limit descriptor list must not be empty"
    );
}

#[tokio::test]
async fn cache_errors_are_reported_as_is() {
    let cache = FakeCache {
        error: Some("cache error".into()),
        ..Default::default()
    };
    let service = RateLimitService::new(Arc::new(cache), ServiceSettings::default());
    service.set_config(load(CONFIG));

    let error = service
        .should_rate_limit(&request("test-domain", &[&[("foo", "bar")]]))
        .await
        .unwrap_err();
    assert_eq!(error, ServiceError::Cache(CacheError("cache error".into())));
    assert_eq!(error.to_string(), "cache error");
}

#[tokio::test]
async fn unlimited_descriptors_skip_the_cache_and_report_max_remaining() {
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::Ok, 9),
        status(Code::Ok, 0),
        status(Code::Ok, 0),
    ]));
    let service = RateLimitService::new(cache.clone(), ServiceSettings::default());
    service.set_config(load(
        "
domain: some-domain
descriptors:
  - key: foo
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: baz
    rate_limit: {unlimited: true}
",
    ));

    let response = service
        .should_rate_limit(&request(
            "some-domain",
            &[&[("foo", "bar")], &[("hello", "world")], &[("baz", "qux")]],
        ))
        .await
        .unwrap();

    assert_eq!(response.overall_code(), Code::Ok);
    assert_eq!(
        response.statuses,
        [
            status(Code::Ok, 9),
            status(Code::Ok, 0),
            status(Code::Ok, u32::MAX)
        ]
    );
    let received = cache.received();
    assert_eq!(
        received[0].iter().map(Option::is_some).collect::<Vec<_>>(),
        [true, false, false]
    );
}

const REPLACES: &str = "
domain: d
descriptors:
  - key: specific
    rate_limit: {unit: minute, requests_per_unit: 1, name: specific, replaces: [{name: generic}]}
  - key: generic
    rate_limit: {unit: minute, requests_per_unit: 2, name: generic}
  - key: unnamed
    rate_limit: {unit: minute, requests_per_unit: 3}
  - key: a
    rate_limit: {unit: minute, requests_per_unit: 4, name: a, replaces: [{name: b}]}
  - key: b
    rate_limit: {unit: minute, requests_per_unit: 5, name: b, replaces: [{name: a}]}
  - key: open
    rate_limit: {unlimited: true, name: open, replaces: [{name: generic}]}
";

async fn checked(descriptors: &[&[(&str, &str)]]) -> Vec<Option<u32>> {
    let cache = Arc::new(FakeCache::returning(vec![
        status(Code::Ok, 0);
        descriptors.len()
    ]));
    let service = RateLimitService::new(cache.clone(), ServiceSettings::default());
    service.set_config(load(REPLACES));
    service
        .should_rate_limit(&request("d", descriptors))
        .await
        .unwrap();
    cache.received()[0]
        .iter()
        .map(|l| l.as_ref().map(|l| l.limit.requests_per_unit))
        .collect()
}

#[tokio::test]
async fn replaced_limits_are_not_checked() {
    assert_eq!(
        checked(&[&[("generic", "x")], &[("specific", "x")]]).await,
        [None, Some(1)]
    );
    assert_eq!(
        checked(&[&[("generic", "x")], &[("unnamed", "x")]]).await,
        [Some(2), Some(3)]
    );
    // Not transitive: both sides of a cycle are dropped.
    assert_eq!(checked(&[&[("a", "x")], &[("b", "x")]]).await, [None, None]);
    // An unlimited limit still replaces others.
    assert_eq!(
        checked(&[&[("generic", "x")], &[("open", "x")]]).await,
        [None, None]
    );
}

async fn overall(
    settings: ServiceSettings,
    config: &str,
    cases: &[(&[(&str, &str)], Code)],
) -> Code {
    let descriptors: Vec<_> = cases.iter().map(|(d, _)| *d).collect();
    let statuses = cases.iter().map(|(_, code)| status(*code, 0)).collect();
    let service = RateLimitService::new(Arc::new(FakeCache::returning(statuses)), settings);
    service.set_config(load(config));
    service
        .should_rate_limit(&request("d", &descriptors))
        .await
        .unwrap()
        .overall_code()
}

const QUOTA: &str = "
domain: d
descriptors:
  - key: regular
    rate_limit: {unit: minute, requests_per_unit: 10}
  - key: backend_name
    quota_mode: true
    rate_limit: {unit: minute, requests_per_unit: 100}
    descriptors:
      - key: tenant
        quota_mode: true
        rate_limit: {unit: minute, requests_per_unit: 10}
";

#[tokio::test]
async fn global_shadow_mode_always_answers_ok() {
    let shadow = ServiceSettings {
        global_shadow_mode: true,
        ..Default::default()
    };
    let cases: &[(&[(&str, &str)], Code)] = &[
        (&[("regular", "x")], Code::OverLimit),
        (&[("other", "x")], Code::Ok),
    ];
    assert_eq!(overall(shadow.clone(), QUOTA, cases).await, Code::Ok);
    assert_eq!(
        overall(ServiceSettings::default(), QUOTA, cases).await,
        Code::OverLimit
    );
    // Also over quota groups.
    let cases: &[(&[(&str, &str)], Code)] = &[(&[("backend_name", "a")], Code::OverLimit)];
    assert_eq!(overall(shadow, QUOTA, cases).await, Code::Ok);
}

#[tokio::test]
async fn quota_groups_are_over_only_when_every_group_is_exhausted() {
    let default = ServiceSettings::default();
    let a_tenant: &[(&str, &str)] = &[("backend_name", "a"), ("tenant", "t1")];
    let a_model: &[(&str, &str)] = &[("backend_name", "a")];
    let b_model: &[(&str, &str)] = &[("backend_name", "b")];

    // Within a group (same backend), any exhausted bucket exhausts the group.
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(a_tenant, Code::OverLimit), (a_model, Code::Ok)]
        )
        .await,
        Code::OverLimit
    );
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(a_tenant, Code::Ok), (a_model, Code::OverLimit)]
        )
        .await,
        Code::OverLimit
    );
    // Across groups, one group with quota left lets the request through (failover).
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[
                (a_tenant, Code::OverLimit),
                (a_model, Code::OverLimit),
                (b_model, Code::Ok)
            ]
        )
        .await,
        Code::Ok
    );
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(a_model, Code::OverLimit), (b_model, Code::OverLimit)]
        )
        .await,
        Code::OverLimit
    );
    // A regular descriptor over its limit is over regardless of quota groups.
    assert_eq!(
        overall(
            default.clone(),
            QUOTA,
            &[(&[("regular", "x")], Code::OverLimit), (b_model, Code::Ok)]
        )
        .await,
        Code::OverLimit
    );
    assert_eq!(
        overall(
            default,
            QUOTA,
            &[(&[("regular", "x")], Code::Ok), (b_model, Code::OverLimit)]
        )
        .await,
        Code::OverLimit
    );
}

#[tokio::test]
async fn global_quota_mode_puts_every_descriptor_in_a_quota_group() {
    let quota = ServiceSettings {
        global_quota_mode: true,
        ..Default::default()
    };
    // Without backend entries, each descriptor is its own group: one with quota left is enough.
    let cases: &[(&[(&str, &str)], Code)] = &[
        (&[("regular", "x")], Code::OverLimit),
        (&[("other", "y")], Code::Ok),
    ];
    assert_eq!(overall(quota.clone(), QUOTA, cases).await, Code::Ok);
    let cases: &[(&[(&str, &str)], Code)] = &[
        (&[("regular", "x")], Code::OverLimit),
        (&[("regular", "y")], Code::OverLimit),
    ];
    assert_eq!(overall(quota, QUOTA, cases).await, Code::OverLimit);
}

#[tokio::test]
async fn a_cache_answer_of_the_wrong_length_is_an_error() {
    // Upstream asserts and crashes the process; per D1 this becomes a clean error.
    for statuses in [vec![status(Code::Ok, 0)], vec![status(Code::Ok, 0); 3]] {
        let service = RateLimitService::new(
            Arc::new(FakeCache::returning(statuses)),
            ServiceSettings::default(),
        );
        service.set_config(load(CONFIG));

        let error = service
            .should_rate_limit(&request(
                "test-domain",
                &[&[("foo", "bar")], &[("hello", "world")]],
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, ServiceError::Cache(_)), "{error:?}");
    }
}
