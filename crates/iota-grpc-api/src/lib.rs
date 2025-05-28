// Include the `tonic::include_proto!` macro output
// This assumes your auto-generated proto files will be in a module structure
// that matches your package name in the .proto files (e.g., iota.gprc.v1)

// Configure how tonic includes the generated files.
// The OUT_DIR environment variable is set by Cargo and points to where
// tonic_build places the generated files. The PROST_ आउट_DIR can be used by
// prost directly if not using tonic_build for some reason.

// pub mod types; // You might have a types.rs for shared Rust types or
// conversions specific to gRPC layer
pub mod conversions;
pub mod error;
pub mod server;
pub mod services;
pub mod utils;

// This module will be populated by tonic_build from your .proto files.
// The name of the module (e.g., "iota_gprc_v1") should match the package
// statement in your .proto files, replacing dots with underscores if that's how
// tonic_build mangles it, or using the exact path. It's common to create a
// module and then use tonic::include_proto! inside it.

pub mod proto {
    pub mod iota {
        pub mod gprc {
            pub mod v1 {
                tonic::include_proto!("iota.gprc.v1"); // Loads all compiled protos under this package
            }
        }
    }
}

// Example re-export (optional, but can be convenient)
// pub use proto::iota::gprc::v1::{
// checkpoint_gprc_service_server::CheckpointGprcServiceServer,
// CheckpointDataGprc,
// GetCheckpointRequest,
// ... other types and server traits
// };
