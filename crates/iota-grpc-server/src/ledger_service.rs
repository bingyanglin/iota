// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    v0::{
        common::BcsData,
        epoch::{Epoch, ProtocolConfig},
        ledger_service::{self as grpc_ledger, ledger_service_server::LedgerService},
    },
};
use iota_protocol_config::{Chain, ProtocolConfig as IotaProtocolConfig};
use iota_types::iota_system_state::IotaSystemStateTrait;
use prost_types::{FieldMask, Timestamp};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::types::GrpcReader;

/// Default fields to return when no read_mask is specified
/// Matches SUI's pattern: epoch metadata, timestamps, gas price, and protocol
/// version
pub const READ_MASK_DEFAULT: &str = "epoch,first_checkpoint,last_checkpoint,start,end,reference_gas_price,protocol_config.protocol_version";

pub struct LedgerGrpcService {
    pub reader: Arc<GrpcReader>,
    pub cancellation_token: CancellationToken,
    pub chain: Chain,
}

impl LedgerGrpcService {
    pub fn new(
        reader: Arc<GrpcReader>,
        cancellation_token: CancellationToken,
        chain: Chain,
    ) -> Self {
        Self {
            reader,
            cancellation_token,
            chain,
        }
    }
}

#[tonic::async_trait]
impl LedgerService for LedgerGrpcService {
    async fn get_epoch(
        &self,
        request: Request<grpc_ledger::GetEpochRequest>,
    ) -> Result<Response<grpc_ledger::GetEpochResponse>, Status> {
        get_epoch(self, request.into_inner()).map(Response::new)
    }
}

/// Standalone get_epoch function following SUI's pattern
/// Handles field masking, epoch resolution, and response building
#[tracing::instrument(skip(service))]
fn get_epoch(
    service: &LedgerGrpcService,
    request: grpc_ledger::GetEpochRequest,
) -> Result<grpc_ledger::GetEpochResponse, Status> {
    // Parse and validate the read_mask
    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));

        // Validate the field mask paths against the Epoch message structure
        read_mask.validate::<Epoch>().map_err(|path| {
            Status::invalid_argument(format!("invalid read_mask path: {}", path))
        })?;

        FieldMaskTree::from(read_mask)
    };

    let mut message = Epoch::default();

    // Determine current epoch from the latest checkpoint
    let current_epoch = service
        .reader
        .get_latest_checkpoint_sequence_number()
        .and_then(|seq| {
            service
                .reader
                .get_checkpoint_summary(seq)
                .ok()
                .flatten()
                .map(|cp| cp.epoch())
        })
        .ok_or_else(|| {
            Status::unavailable("Unable to determine current epoch: no checkpoints available")
        })?;

    let epoch = request.epoch.unwrap_or(current_epoch);

    // Handle system state - check if current epoch and field requested
    let system_state =
        if epoch == current_epoch && read_mask.contains(Epoch::BCS_SYSTEM_STATE_FIELD.name) {
            match service.reader.get_system_state() {
                Ok(state) => Some(state),
                Err(e) => {
                    debug!(
                        "Failed to get current system state for epoch {}: {}",
                        epoch, e
                    );
                    None
                }
            }
        } else {
            None
        };

    // Populate epoch number if requested
    if read_mask.contains(Epoch::EPOCH_FIELD.name) {
        message.epoch = Some(epoch);
    }

    // Get epoch info from storage (uses IOTA's storage adapter)
    if let Some(epoch_info) = get_epoch_info_optional(&service.reader, epoch) {
        if read_mask.contains(Epoch::FIRST_CHECKPOINT_FIELD.name) {
            message.first_checkpoint = epoch_info.first_checkpoint;
        }

        if read_mask.contains(Epoch::LAST_CHECKPOINT_FIELD.name) {
            message.last_checkpoint = epoch_info.last_checkpoint;
        }

        if read_mask.contains(Epoch::START_FIELD.name) {
            message.start = epoch_info.start_timestamp;
        }

        if read_mask.contains(Epoch::END_FIELD.name) {
            message.end = epoch_info.end_timestamp;
        }

        if read_mask.contains(Epoch::REFERENCE_GAS_PRICE_FIELD.name) {
            message.reference_gas_price = epoch_info.reference_gas_price;
        }

        // Handle protocol_config with subtree masking
        if let Some(submask) = read_mask.subtree(Epoch::PROTOCOL_CONFIG_FIELD.name) {
            if let Some(protocol_version) = epoch_info.protocol_version {
                match get_protocol_config(protocol_version, service.chain) {
                    Ok(config) => {
                        // Apply field masking to protocol config
                        message.protocol_config =
                            Some(apply_protocol_config_mask(config, &submask));
                    }
                    Err(e) => {
                        return Err(Status::from(e));
                    }
                }
            }
        }

        // If we're not loading the current epoch, grab the indexed snapshot of
        // system state (Note: IOTA doesn't have indexed system state
        // yet, so we skip this for now)
    }

    // Handle bcs_system_state field
    if let Some(system_state) = system_state {
        if read_mask.contains(Epoch::BCS_SYSTEM_STATE_FIELD.name) {
            match bcs::to_bytes(&system_state) {
                Ok(bcs_bytes) => {
                    message.bcs_system_state = Some(BcsData { data: bcs_bytes });
                }
                Err(e) => {
                    warn!(
                        "Failed to serialize system state to BCS for epoch {}: {}",
                        epoch, e
                    );
                }
            }
        }
    }

    // Handle committee field
    if read_mask.contains(Epoch::COMMITTEE_FIELD.name) {
        let committee = service
            .reader
            .get_committee(epoch)
            .map_err(|e| Status::internal(format!("Failed to get committee: {}", e)))?
            .ok_or_else(|| CommitteeNotFoundError::new(epoch))?;
        message.committee = Some(committee_to_proto(committee.as_ref()));
    }

    Ok(grpc_ledger::GetEpochResponse::new(message))
}

/// Committee not found error
#[derive(Debug)]
pub struct CommitteeNotFoundError {
    epoch: u64,
}

impl CommitteeNotFoundError {
    pub fn new(epoch: u64) -> Self {
        Self { epoch }
    }
}

impl std::fmt::Display for CommitteeNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Committee for epoch {} not found", self.epoch)
    }
}

impl std::error::Error for CommitteeNotFoundError {}

impl From<CommitteeNotFoundError> for Status {
    fn from(value: CommitteeNotFoundError) -> Self {
        Status::not_found(value.to_string())
    }
}

/// Protocol version not found error
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

impl From<ProtocolVersionNotFoundError> for Status {
    fn from(value: ProtocolVersionNotFoundError) -> Self {
        Status::not_found(value.to_string())
    }
}

/// Get protocol config for a specific version
fn get_protocol_config(
    version: u64,
    chain: Chain,
) -> Result<ProtocolConfig, ProtocolVersionNotFoundError> {
    use iota_protocol_config::ProtocolVersion;

    let protocol_version = ProtocolVersion::new(version);
    let config = IotaProtocolConfig::get_for_version_if_supported(protocol_version, chain)
        .ok_or_else(|| ProtocolVersionNotFoundError::new(version))?;

    Ok(protocol_config_to_proto(config))
}

/// Convert IOTA ProtocolConfig to protobuf ProtocolConfig
pub fn protocol_config_to_proto(config: IotaProtocolConfig) -> ProtocolConfig {
    use iota_protocol_config::ProtocolConfigValue;

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
    message.feature_flags = feature_flags;
    message.attributes = attributes;
    message
}

/// Apply field masking to protocol config
/// For now, this returns the full config since we don't have merge support yet
fn apply_protocol_config_mask(config: ProtocolConfig, _submask: &FieldMaskTree) -> ProtocolConfig {
    // TODO: Implement proper field masking using merge_from like SUI
    // For now, return the full config
    config
}

/// Epoch information collected from storage (adapted for IOTA)
struct EpochInfo {
    first_checkpoint: Option<u64>,
    last_checkpoint: Option<u64>,
    start_timestamp: Option<Timestamp>,
    end_timestamp: Option<Timestamp>,
    reference_gas_price: Option<u64>,
    protocol_version: Option<u64>,
}

/// Get epoch information from storage, returning None if unavailable
fn get_epoch_info_optional(reader: &GrpcReader, epoch_id: u64) -> Option<EpochInfo> {
    // Get the last checkpoint of this epoch
    let last_checkpoint_summary = reader.get_epoch_last_checkpoint(epoch_id).ok()??;

    let last_seq = *last_checkpoint_summary.sequence_number();

    // Calculate first checkpoint of this epoch
    let first_seq = if epoch_id > 0 {
        match reader.get_epoch_last_checkpoint(epoch_id - 1) {
            Ok(Some(prev_epoch_last)) => *prev_epoch_last.sequence_number() + 1,
            _ => return None,
        }
    } else {
        0 // Epoch 0 starts at checkpoint 0
    };

    // Get timestamps
    let start_ts = reader.get_checkpoint_summary(first_seq).ok()??.timestamp_ms;
    let start_ts = Some(timestamp_ms_to_proto(start_ts));

    let end_ts = Some(timestamp_ms_to_proto(last_checkpoint_summary.timestamp_ms));

    // Get reference gas price and protocol version from system state
    let (reference_gas_price, protocol_version) = match reader.get_system_state() {
        Ok(system_state) => (
            Some(system_state.reference_gas_price()),
            Some(system_state.protocol_version()),
        ),
        Err(_) => (None, None),
    };

    Some(EpochInfo {
        first_checkpoint: Some(first_seq),
        last_checkpoint: Some(last_seq),
        start_timestamp: start_ts,
        end_timestamp: end_ts,
        reference_gas_price,
        protocol_version,
    })
}

/// Convert millisecond timestamp to protobuf Timestamp
fn timestamp_ms_to_proto(timestamp_ms: u64) -> Timestamp {
    Timestamp {
        seconds: (timestamp_ms / 1000) as i64,
        nanos: ((timestamp_ms % 1000) * 1_000_000) as i32,
    }
}

/// Convert Committee to protobuf ValidatorCommittee
fn committee_to_proto(
    committee: &iota_types::committee::Committee,
) -> iota_grpc_types::v0::epoch::ValidatorCommittee {
    use iota_grpc_types::v0::epoch::{ValidatorCommittee, ValidatorCommitteeMember};
    use iota_types::crypto::ToFromBytes;

    let members = committee
        .voting_rights
        .iter()
        .map(|(public_key, weight)| ValidatorCommitteeMember {
            public_key: Some(public_key.as_bytes().to_vec()),
            weight: Some(*weight),
        })
        .collect();

    ValidatorCommittee {
        epoch: Some(committee.epoch),
        members,
    }
}
