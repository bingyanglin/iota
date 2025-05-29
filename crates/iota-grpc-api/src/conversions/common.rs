// Placeholder for common conversion utilities
// May be used for types like Direction, etc. if they need gRPC-specific
// conversions.

use crate::proto::iota::gprc::v1::CheckpointDigestGprc;

pub fn convert_core_checkpoint_digest_to_gprc(
    core_digest: &iota_types::digests::CheckpointDigest,
) -> CheckpointDigestGprc {
    CheckpointDigestGprc {
        digest: core_digest.into_inner().to_vec(),
    }
}
