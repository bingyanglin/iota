// conversions specific to gRPC layer
pub mod conversions;
pub mod error;
pub mod server;
pub mod services;

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
