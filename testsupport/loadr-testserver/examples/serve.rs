//! Run the HTTP and gRPC test servers standalone (demos, manual testing):
//!
//!   cargo run -p loadr-testserver --example serve [-- <port>]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = loadr_testserver::HttpTestServer::spawn().await?;
    println!("test server listening on {}", server.base_url());
    println!("endpoints: / /json /xml /html /echo /status/<n> /delay/<ms> /cookies /gzip /redirect/<n> /login /large/<kb> /headers /counter");
    let grpc = loadr_testserver::GrpcEchoServer::spawn().await?;
    println!("grpc echo server listening on grpc://{}", grpc.addr);
    println!("grpc service: loadr.test.Echo (UnaryEcho/ServerStreamEcho/ClientStreamEcho/BidiEcho, reflection v1)");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
