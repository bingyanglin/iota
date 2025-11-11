// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    field::{FieldMaskTree, FieldMaskUtil},
    v0::ledger_service::{GetServiceInfoRequest, GetServiceInfoResponse},
};
use iota_protocol_config::Chain;
use iota_types::digests::ChainIdentifier;
use prost_types::{FieldMask, Timestamp};
use tonic::Status;

use crate::types::GrpcReader;

/// Default fields to read if no mask is specified
pub const READ_MASK_DEFAULT: &str = "chain_id,epoch,executed_checkpoint_height";

/// Convert millisecond timestamp to protobuf Timestamp
fn timestamp_ms_to_proto(timestamp_ms: u64) -> Timestamp {
    Timestamp {
        seconds: (timestamp_ms / 1000) as i64,
        nanos: ((timestamp_ms % 1000) * 1_000_000) as i32,
    }
}

/// Get service information about the current state of the node
#[tracing::instrument(skip(reader))]
pub async fn get_service_info(
    reader: GrpcReader,
    chain_id: ChainIdentifier,
    chain: Chain,
    server_version: Option<String>,
    request: GetServiceInfoRequest,
) -> Result<GetServiceInfoResponse, Status> {
    // Parse field mask (without validation for now, as field constants aren't
    // generated yet)
    let read_mask = {
        let read_mask = request
            .read_mask
            .unwrap_or_else(|| FieldMask::from_str(READ_MASK_DEFAULT));
        FieldMaskTree::from(read_mask)
    };

    // Get latest checkpoint to determine current state
    let latest_checkpoint_seq = reader
        .get_latest_checkpoint_sequence_number()
        .ok_or_else(|| Status::unavailable("No checkpoints available"))?;

    let checkpoint = reader
        .get_checkpoint_summary(latest_checkpoint_seq)
        .map_err(|e| Status::internal(format!("Failed to get checkpoint summary: {e}")))?
        .ok_or_else(|| Status::not_found("Latest checkpoint not found"))?;

    let mut message = GetServiceInfoResponse::default();

    // Populate fields based on read mask
    if read_mask.contains("chain_id") {
        // Convert chain_id bytes to hex string
        message.chain_id = Some(format!("0x{chain_id}"));
    }

    if read_mask.contains("chain") {
        message.chain = Some(chain.as_str().to_string());
    }

    if read_mask.contains("epoch") {
        message.epoch = Some(checkpoint.epoch());
    }

    if read_mask.contains("executed_checkpoint_height") {
        message.executed_checkpoint_height = Some(*checkpoint.sequence_number());
    }

    if read_mask.contains("executed_checkpoint_timestamp") {
        message.executed_checkpoint_timestamp =
            Some(timestamp_ms_to_proto(checkpoint.timestamp_ms));
    }

    if read_mask.contains("lowest_available_checkpoint") {
        // TODO: Implement proper tracking of pruned checkpoints
        message.lowest_available_checkpoint = Some(0);
    }

    if read_mask.contains("lowest_available_checkpoint_objects") {
        // TODO: Implement proper tracking of pruned objects
        message.lowest_available_checkpoint_objects = Some(0);
    }

    if read_mask.contains("server") {
        message.server = server_version;
    }

    Ok(message)
}
