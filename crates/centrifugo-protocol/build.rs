//! Generate Rust protobuf types from `proto/client.proto`.
//!
//! Uses the pure-Rust `protox` compiler (no external `protoc` binary required),
//! feeding the resulting FileDescriptorSet to prost-build.

fn main() {
    let fds = protox::compile(["proto/client.proto", "proto/control.proto"], ["proto"])
        .expect("protox failed to compile protos");

    let mut config = prost_build::Config::new();
    config
        .compile_fds(fds)
        .expect("prost-build failed to generate Rust from FileDescriptorSet");

    println!("cargo:rerun-if-changed=proto/client.proto");
    println!("cargo:rerun-if-changed=proto/control.proto");
    println!("cargo:rerun-if-changed=build.rs");
}
