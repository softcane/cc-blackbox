fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(
            &[
                "proto/envoy/service/ext_proc/v3/external_processor.proto",
                "proto/envoy/config/core/v3/base.proto",
                "proto/envoy/type/v3/http_status.proto",
                "proto/envoy/extensions/filters/http/ext_proc/v3/processing_mode.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
