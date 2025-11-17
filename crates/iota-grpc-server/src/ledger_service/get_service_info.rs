// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{
    proto_helpers::timestamp_ms_to_proto,
    v0::ledger_service::{GetServiceInfoRequest, GetServiceInfoResponse},
};
use iota_types::digests::{ChainIdentifier, Digest};
use tap::Pipe;

use crate::{error::Result, types::GrpcReader};

pub(crate) fn get_service_info(
    reader: GrpcReader,
    chain_id: ChainIdentifier,
    server_version: Option<String>,
    _request: GetServiceInfoRequest,
) -> Result<GetServiceInfoResponse> {
    let latest_checkpoint = reader.get_latest_checkpoint()?;
    let lowest_available_checkpoint = reader.get_lowest_available_checkpoint()?.pipe(Some);
    let lowest_available_checkpoint_objects =
        reader.get_lowest_available_checkpoint_objects()?.pipe(Some);

    let message = GetServiceInfoResponse {
        chain_id: Some(Digest::new(chain_id.as_bytes().to_owned()).to_string()),
        chain: Some(chain_id.chain().as_str().into()),
        epoch: Some(latest_checkpoint.epoch()),
        executed_checkpoint_height: Some(latest_checkpoint.sequence_number),
        executed_checkpoint_timestamp: Some(timestamp_ms_to_proto(latest_checkpoint.timestamp_ms)),
        lowest_available_checkpoint,
        lowest_available_checkpoint_objects,
        server: server_version,
    };

    Ok(message)
}
