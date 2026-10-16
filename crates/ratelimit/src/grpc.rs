//! The `envoy.service.ratelimit.v3.RateLimitService` gRPC endpoint.

use crate::service::{RateLimitCache, RateLimitService};
use ratelimit_proto::envoy::service::ratelimit::v3::{
    RateLimitRequest, RateLimitResponse,
    rate_limit_service_server::{self, RateLimitServiceServer},
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct GrpcService<C> {
    service: Arc<RateLimitService<C>>,
}

/// The gRPC server for `service`.
pub fn server<C: RateLimitCache>(
    service: Arc<RateLimitService<C>>,
) -> RateLimitServiceServer<GrpcService<C>> {
    RateLimitServiceServer::new(GrpcService { service })
}

#[tonic::async_trait]
impl<C: RateLimitCache> rate_limit_service_server::RateLimitService for GrpcService<C> {
    async fn should_rate_limit(
        &self,
        request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        // Upstream returns plain Go errors, which grpc-go sends as UNKNOWN with the error text.
        self.service
            .should_rate_limit(request.get_ref())
            .await
            .map(Response::new)
            .map_err(|e| Status::unknown(e.to_string()))
    }
}
