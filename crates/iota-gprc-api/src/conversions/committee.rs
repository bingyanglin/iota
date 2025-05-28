use iota_types::committee::{Committee as CoreCommittee, TOTAL_VOTING_POWER};

use crate::{
    error::GrpcApiError,
    proto::iota::gprc::v1::{CommitteeGprc, CommitteeMemberGprc, EpochIdGprc, StringU64},
};

pub fn convert_core_committee_to_gprc(
    core_committee: &CoreCommittee,
) -> Result<CommitteeGprc, GrpcApiError> {
    let mut members_gprc = Vec::new();
    for (authority_name_bytes, stake_unit) in &core_committee.voting_rights {
        members_gprc.push(CommitteeMemberGprc {
            authority_name: hex::encode(authority_name_bytes.as_ref()),
            stake_units: Some(StringU64 {
                value: (*stake_unit).to_string(),
            }),
        });
    }

    Ok(CommitteeGprc {
        epoch_id: Some(EpochIdGprc {
            epoch: core_committee.epoch(),
        }),
        members: members_gprc,
        total_stake: Some(StringU64 {
            value: TOTAL_VOTING_POWER.to_string(),
        }),
    })
}
