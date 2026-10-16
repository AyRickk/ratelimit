//! The `envoy.service.ratelimit.v3.RateLimitService` gRPC endpoint, called over a real socket.

use ratelimit::config::{self, ConfigFile, LoadOptions, RateLimit};
use ratelimit::grpc;
use ratelimit::limiter::SystemClock;
use ratelimit::service::{CacheError, RateLimitCache, RateLimitService, ServiceSettings};
use ratelimit_proto::envoy::extensions::common::ratelimit::v3::{
    RateLimitDescriptor, rate_limit_descriptor::Entry,
};
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, rate_limit_response::Code, rate_limit_response::DescriptorStatus,
    rate_limit_service_client::RateLimitServiceClient,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Answers OVER_LIMIT for descriptors with a limit, OK otherwise.
struct OverWhenLimited;

impl RateLimitCache for OverWhenLimited {
    async fn do_limit(
        &self,
        _request: &RateLimitRequest,
        limits: &[Option<RateLimit>],
    ) -> Result<Vec<DescriptorStatus>, CacheError> {
        Ok(limits
            .iter()
            .map(|limit| DescriptorStatus {
                code: if limit.is_some() {
                    Code::OverLimit
                } else {
                    Code::Ok
                } as i32,
                current_limit: limit.as_ref().map(|l| l.limit.clone()),
                ..Default::default()
            })
            .collect())
    }
}

async fn client() -> RateLimitServiceClient<tonic::transport::Channel> {
    let service = Arc::new(RateLimitService::new(
        OverWhenLimited,
        Arc::new(SystemClock),
        ServiceSettings::default(),
    ));
    let file = ConfigFile {
        name: "inline.yaml".into(),
        content: "
domain: test-domain
descriptors:
  - key: foo
    rate_limit: {unit: minute, requests_per_unit: 10, name: foo_limit}
"
        .into(),
    };
    service.set_config(config::load(&[file], &LoadOptions::default()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(grpc::server(service))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    RateLimitServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap()
}

fn request(domain: &str, entries: &[(&str, &str)]) -> RateLimitRequest {
    RateLimitRequest {
        domain: domain.into(),
        descriptors: vec![RateLimitDescriptor {
            entries: entries
                .iter()
                .map(|(key, value)| Entry {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            ..Default::default()
        }],
        hits_addend: 1,
    }
}

#[tokio::test]
async fn should_rate_limit_is_served_over_grpc() {
    let mut client = client().await;

    let response = client
        .should_rate_limit(request("test-domain", &[("foo", "bar")]))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.overall_code(), Code::OverLimit);
    assert_eq!(
        response.statuses[0].current_limit.as_ref().unwrap().name,
        "foo_limit"
    );
}

#[tokio::test]
async fn service_errors_are_unknown_status_with_upstream_message() {
    let mut client = client().await;

    let status = client
        .should_rate_limit(request("", &[("foo", "bar")]))
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unknown);
    assert_eq!(status.message(), "rate limit domain must not be empty");
}

#[tokio::test]
async fn invalid_arguments_are_invalid_argument_status() {
    let mut client = client().await;
    let mut request = request("test-domain", &[("foo", "bar")]);
    request.descriptors[0].limit = Some(
        ratelimit_proto::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride {
            requests_per_unit: 1,
            unit: 0,
            unit_multiplier: None,
        },
    );

    let status = client.should_rate_limit(request).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}
