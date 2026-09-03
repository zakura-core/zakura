//! Generates the lightwalletd gRPC test client.

pub fn generate_client() {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(
            &["tests/common/lightwalletd/proto/service.proto"],
            &["tests/common/lightwalletd/proto"],
        )
        .expect("lightwalletd test protobufs should compile");
}
