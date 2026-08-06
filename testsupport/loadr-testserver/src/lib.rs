//! In-process gRPC echo server for integration and performance tests.
//!
//! [`GrpcEchoServer`] implements unary, client-streaming, server-streaming,
//! and bidirectional-streaming RPCs, reflection, and optional TLS.

mod error;
mod grpc;

pub use error::TestServerError;
pub use grpc::{pb, GrpcEchoServer, FILE_DESCRIPTOR_SET};
