// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

fn main() {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/node.proto"], &["proto"])
        .unwrap();
}
