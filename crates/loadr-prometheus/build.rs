//! Compile the vendored Prometheus remote-write protobuf definitions.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/prometheus/types.proto",
        "proto/prometheus/remote.proto",
    ];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let fds = protox::compile(protos, ["proto"])?;
    prost_build::Config::new().compile_fds(fds)?;
    Ok(())
}
