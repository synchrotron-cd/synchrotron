// Compile the sidecar gRPC protocol. We bundle `protoc` via
// `protoc-bin-vendored` so the build works on hosts where the
// system has no protoc installed (CI images, minimal dev boxes).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/plugin.proto"], &["proto"])?;
    Ok(())
}
