//! Compile `proto/api.proto` into tonic server + client + prost message types.
//! Uses pure-Rust `protox` to produce the FileDescriptorSet (no `protoc`), then
//! hands it to tonic-build.

fn main() {
    let fds = protox::compile(["proto/api.proto"], ["proto"]).expect("protox compile api.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_fds(fds)
        .expect("tonic-build compile_fds");
    println!("cargo:rerun-if-changed=proto/api.proto");
    println!("cargo:rerun-if-changed=build.rs");
}
