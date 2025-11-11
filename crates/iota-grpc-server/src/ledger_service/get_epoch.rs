// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    merge::Merge,
    proto_helpers::timestamp_ms_to_proto,
    v0::{
        bcs::BcsData,
        epoch::{Epoch, ProtocolConfig},
        ledger_service::{GetEpochRequest, GetEpochResponse},
    },
};
use iota_protocol_config::{Chain, ProtocolConfig as IotaProtocolConfig, ProtocolConfigValue};
use iota_types::committee::EpochId;
use prost_types::FieldMask;

use crate::{
    error::{ErrorReason, FieldViolation, Result},
    ledger_service::LedgerGrpcService,
};

pub const READ_MASK_DEFAULT: &str = "epoch,first_checkpoint,last_checkpoint,start,end,reference_gas_price,protocol_config.protocol_version";

#[tracing::instrument(skip(service))]
pub fn get_epoch(
    service: &LedgerGrpcService,
    request: GetEpochRequest,
) -> Result<GetEpochResponse> {
    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));
        read_mask.validate::<Epoch>().map_err(|path| {
            FieldViolation::new("read_mask")
                .with_description(format!("invalid read_mask path: {path}"))
                .with_reason(ErrorReason::FieldInvalid)
        })?;
        FieldMaskTree::from(read_mask)
    };

    let mut message = Epoch::default();

    let current_epoch = service.reader.get_latest_checkpoint()?.epoch();
    let epoch = request.epoch.unwrap_or(current_epoch);

    let mut system_state =
        if epoch == current_epoch && read_mask.contains(Epoch::BCS_SYSTEM_STATE_FIELD.name) {
            Some(service.reader.get_system_state()?)
        } else {
            None
        };

    if read_mask.contains(Epoch::EPOCH_FIELD.name) {
        message.epoch = Some(epoch);
    }

    if let Some(epoch_info) = service.reader.state_reader.get_epoch_info(epoch) {
        if read_mask.contains(Epoch::FIRST_CHECKPOINT_FIELD.name) {
            message.first_checkpoint = epoch_info.start_checkpoint;
        }

        if read_mask.contains(Epoch::LAST_CHECKPOINT_FIELD.name) {
            message.last_checkpoint = epoch_info.end_checkpoint;
        }

        if read_mask.contains(Epoch::START_FIELD.name) {
            message.start = epoch_info.start_timestamp_ms.map(timestamp_ms_to_proto);
        }

        if read_mask.contains(Epoch::END_FIELD.name) {
            message.end = epoch_info.end_timestamp_ms.map(timestamp_ms_to_proto);
        }

        if read_mask.contains(Epoch::REFERENCE_GAS_PRICE_FIELD.name) {
            message.reference_gas_price = epoch_info.reference_gas_price;
        }

        if let Some(submask) = read_mask.subtree(Epoch::PROTOCOL_CONFIG_FIELD.name) {
            let chain = service.chain;
            let protocol_config = epoch_info
                .protocol_version
                .map(|version| get_protocol_config(version, chain))
                .transpose()?;

            message.protocol_config =
                protocol_config.map(|config| ProtocolConfig::merge_from(config, &submask));
        }

        // If we're not loading the current epoch then grab the indexed snapshot of the
        // system state at the start of the epoch.
        if system_state.is_none() {
            system_state = epoch_info.system_state;
        }
    }

    if let Some(system_state) = system_state {
        if read_mask.contains(Epoch::BCS_SYSTEM_STATE_FIELD.name) {
            let bcs_bytes = bcs::to_bytes(&system_state).map_err(|e| {
                crate::error::RpcError::Internal(format!(
                    "Failed to serialize system state to BCS: {value}",
                    value = e
                ))
            })?;
            message.bcs_system_state = Some(BcsData { data: bcs_bytes });
        }
    }

    if read_mask.contains(Epoch::COMMITTEE_FIELD.name) {
        message.committee = Some(
            service
                .reader
                .get_committee(epoch)?
                .ok_or_else(|| CommitteeNotFoundError::new(epoch))?
                .as_ref()
                .into(),
        );
    }

    Ok(GetEpochResponse::new(message))
}

#[derive(Debug)]
pub struct CommitteeNotFoundError {
    epoch: EpochId,
}

impl CommitteeNotFoundError {
    pub fn new(epoch: EpochId) -> Self {
        Self { epoch }
    }
}

impl std::fmt::Display for CommitteeNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Committee for epoch {} not found", self.epoch)
    }
}

impl std::error::Error for CommitteeNotFoundError {}

impl From<CommitteeNotFoundError> for crate::error::RpcError {
    fn from(value: CommitteeNotFoundError) -> Self {
        Self::NotFound(value.to_string())
    }
}

#[derive(Debug)]
struct ProtocolVersionNotFoundError {
    version: u64,
}

impl ProtocolVersionNotFoundError {
    pub fn new(version: u64) -> Self {
        Self { version }
    }
}

impl std::fmt::Display for ProtocolVersionNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Protocol version {} not found", self.version)
    }
}

impl std::error::Error for ProtocolVersionNotFoundError {}

impl From<ProtocolVersionNotFoundError> for crate::error::RpcError {
    fn from(value: ProtocolVersionNotFoundError) -> Self {
        Self::NotFound(value.to_string())
    }
}

fn get_protocol_config(
    version: u64,
    chain: Chain,
) -> std::result::Result<ProtocolConfig, ProtocolVersionNotFoundError> {
    let config = IotaProtocolConfig::get_for_version_if_supported(version.into(), chain)
        .ok_or_else(|| ProtocolVersionNotFoundError::new(version))?;
    Ok(protocol_config_to_proto(config))
}

pub fn protocol_config_to_proto(config: IotaProtocolConfig) -> ProtocolConfig {
    let protocol_version = config.version.as_u64();
    let attributes = config
        .attr_map()
        .into_iter()
        .filter_map(|(k, maybe_v)| {
            maybe_v.map(move |v| {
                let v = match v {
                    ProtocolConfigValue::u16(x) => x.to_string(),
                    ProtocolConfigValue::u32(y) => y.to_string(),
                    ProtocolConfigValue::u64(z) => z.to_string(),
                    ProtocolConfigValue::bool(b) => b.to_string(),
                };
                (k, v)
            })
        })
        .collect();
    let feature_flags = config.feature_map().into_iter().collect();
    let mut message = ProtocolConfig::default();
    message.protocol_version = Some(protocol_version);
    message.feature_flags = Some(iota_grpc_types::v0::epoch::ProtocolFeatureFlags {
        flags: feature_flags,
    });
    message.attributes = Some(iota_grpc_types::v0::epoch::ProtocolAttributes { attributes });
    message
}
