//! Standalone gRPC echo server for local perf runs (loopback, TLS off).
//!
//! Prints `LISTENING <addr>` once ready and serves until Ctrl-C, so an A/B
//! harness can capture the ephemeral port and generate plan URLs from it.

use loadr_testserver::GrpcEchoServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = GrpcEchoServer::spawn().await?;
    println!("LISTENING {}", server.addr);
    tokio::signal::ctrl_c().await?;
    drop(server);
    Ok(())
}
