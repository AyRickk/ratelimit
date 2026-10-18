//! The `envoy.service.ratelimit.v3.RateLimitService` gRPC endpoint.

use crate::service::{RateLimitCache, RateLimitService, ServiceError};
use crate::stats::ServerStats;
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_service_server::{self, RateLimitServiceServer},
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct GrpcService<C> {
    service: Arc<RateLimitService<C>>,
    stats: ServerStats,
}

/// The gRPC server for `service`, its calls counted in `stats`.
pub fn server<C: RateLimitCache>(
    service: Arc<RateLimitService<C>>,
    stats: ServerStats,
) -> RateLimitServiceServer<GrpcService<C>> {
    RateLimitServiceServer::new(GrpcService { service, stats })
}

#[tonic::async_trait]
impl<C: RateLimitCache> rate_limit_service_server::RateLimitService for GrpcService<C> {
    async fn should_rate_limit(
        &self,
        request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        // Upstream returns plain Go errors, which grpc-go sends as UNKNOWN with the error text.
        // The server span is the parent of the request's spans.
        let parent = request
            .extensions()
            .get::<crate::trace::SpanContext>()
            .cloned();
        let call =
            crate::trace::with_parent(parent, self.service.should_rate_limit(request.get_ref()));
        self.stats
            .record("ShouldRateLimit", call)
            .await
            .map(Response::new)
            .map_err(|e| match e {
                ServiceError::InvalidArgument(message) => Status::invalid_argument(message),
                e => Status::unknown(e.to_string()),
            })
    }
}
