// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! gRPC-specific versioned types for forward compatibility.
//!
//! These types provide versioning for gRPC streaming while positioning
//! for future core type evolution. When core types themselves
//! need versioning, these wrappers will evolve naturally.

pub mod proto;

// Generated protobuf modules with field constants
mod proto_generated {
    pub mod google {
        pub mod rpc {
            include!("proto/generated/google.rpc.rs");
        }
    }

    pub mod bcs {
        include!("proto/generated/iota.grpc.v0.bcs.rs");
        include!("proto/generated/iota.grpc.v0.bcs.field_info.rs");
    }

    pub mod checkpoint {
        include!("proto/generated/iota.grpc.v0.checkpoint.rs");
        include!("proto/generated/iota.grpc.v0.checkpoint.field_info.rs");
    }

    pub mod command {
        include!("proto/generated/iota.grpc.v0.command.rs");
        include!("proto/generated/iota.grpc.v0.command.field_info.rs");
    }

    pub mod epoch {
        include!("proto/generated/iota.grpc.v0.epoch.rs");
        include!("proto/generated/iota.grpc.v0.epoch.field_info.rs");
    }

    pub mod event {
        include!("proto/generated/iota.grpc.v0.event.rs");
        include!("proto/generated/iota.grpc.v0.event.field_info.rs");
    }

    pub mod filter {
        include!("proto/generated/iota.grpc.v0.filter.rs");
        include!("proto/generated/iota.grpc.v0.filter.field_info.rs");
    }

    pub mod ledger_service {
        include!("proto/generated/iota.grpc.v0.ledger_service.rs");
        include!("proto/generated/iota.grpc.v0.ledger_service.field_info.rs");
    }

    pub mod object {
        include!("proto/generated/iota.grpc.v0.object.rs");
        include!("proto/generated/iota.grpc.v0.object.field_info.rs");
    }

    pub mod signatures {
        include!("proto/generated/iota.grpc.v0.signatures.rs");
        include!("proto/generated/iota.grpc.v0.signatures.field_info.rs");
    }

    pub mod transaction_execution_service {
        include!("proto/generated/iota.grpc.v0.transaction_execution_service.rs");
        include!("proto/generated/iota.grpc.v0.transaction_execution_service.field_info.rs");
    }

    pub mod transaction {
        include!("proto/generated/iota.grpc.v0.transaction.rs");
        include!("proto/generated/iota.grpc.v0.transaction.field_info.rs");
    }

    pub mod types {
        include!("proto/generated/iota.grpc.v0.types.rs");
        include!("proto/generated/iota.grpc.v0.types.field_info.rs");
    }
}

// Re-export under v0 namespace
pub mod google {
    pub mod rpc {
        pub use super::proto_generated::google::rpc::*;
    }
}
pub mod v0 {
    pub use super::proto_generated::*;
}

pub mod bcs;
pub mod checkpoints;
pub mod epoch;
pub mod events;
pub mod field;
pub mod merge;
pub mod proto_helpers;
