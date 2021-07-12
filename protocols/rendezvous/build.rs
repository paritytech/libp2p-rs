fn main() {
    prost_build::compile_protos(&["src/rpc.proto"], &["src"]).unwrap();
}
