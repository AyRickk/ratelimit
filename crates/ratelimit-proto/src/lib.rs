//! Envoy rate limit service protos, compiled with `protox` (no `protoc` needed).

pub mod envoy {
    pub mod config {
        pub mod core {
            pub mod v3 {
                include!(concat!(env!("OUT_DIR"), "/envoy.config.core.v3.rs"));
            }
        }
    }
    pub mod extensions {
        pub mod common {
            pub mod ratelimit {
                pub mod v3 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/envoy.extensions.common.ratelimit.v3.rs"
                    ));
                }
            }
        }
    }
    pub mod service {
        pub mod ratelimit {
            pub mod v3 {
                include!(concat!(env!("OUT_DIR"), "/envoy.service.ratelimit.v3.rs"));
            }
        }
    }
    pub mod r#type {
        pub mod v3 {
            include!(concat!(env!("OUT_DIR"), "/envoy.r#type.v3.rs"));
        }
    }
}

/// gRPC's health checking protocol (`grpc.health.v1`).
pub mod grpc {
    pub mod health {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/grpc.health.v1.rs"));
        }
    }
}
