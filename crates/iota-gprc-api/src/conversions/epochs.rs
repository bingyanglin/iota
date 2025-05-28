use iota_types::committee::Committee;

use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{EpochIdGprc, EpochInfoGprc, StringU64},
};

pub fn convert_core_committee_to_epoch_info_gprc(
    committee: &Committee,
) -> Result<EpochInfoGprc, GrpcApiError> {
    Ok(EpochInfoGprc {
        epoch_id: Some(EpochIdGprc {
            epoch: committee.epoch,
        }),
        total_stake: Some(StringU64 {
            value: committee.total_votes().to_string(),
        }),
        // The following fields are not directly available from iota_types::committee::Committee
        // via the current RestStateReader interface. They would require access to a more
        // comprehensive epoch data source (e.g., IotaSystemState or EpochStartSystemState).
        start_time_ms: None,        // Placeholder
        end_time_ms: None,          // Placeholder
        rewards_pool_balance: None, // Placeholder
    })
}
