// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

fn main() {
    tonic_build::configure()
        .compile_protos(
            &[
                "proto/common.proto",
                "proto/node.proto",
                "proto/checkpoint.proto",
                "proto/event.proto",
                "proto/transaction.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
