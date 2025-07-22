// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_grpc_types::{CertifiedCheckpointSummary, CheckpointData};
use tonic::transport::Channel;

use crate::node::node_service_client::NodeServiceClient;

/// Enum representing the content of a checkpoint, either full data or summary.
pub enum CheckpointContent {
    Data(iota_types::full_checkpoint_content::CheckpointData),
    Summary(iota_types::messages_checkpoint::CertifiedCheckpointSummary),
}
/// Shared gRPC client for IOTA node operations.
pub struct GrpcNodeClient {
    client: NodeServiceClient<Channel>,
}

impl GrpcNodeClient {
    pub async fn connect(url: &str) -> Result<Self, tonic::transport::Error> {
        let client = NodeServiceClient::connect(url.to_string()).await?;
        Ok(Self { client })
    }

    /// Stream checkpoints from the IOTA node with flexible range options.
    pub async fn stream_checkpoints(
        &mut self,
        start: Option<u64>,
        end: Option<u64>,
        full: Option<bool>,
    ) -> Result<tonic::Streaming<crate::node::Checkpoint>, tonic::Status> {
        let request = crate::node::CheckpointStreamRequest {
            start_sequence_number: start,
            end_sequence_number: end,
            full,
        };
        let response = self.client.stream_checkpoints(request).await?;
        Ok(response.into_inner())
    }

    /// Get the first checkpoint sequence number for a given epoch.
    pub async fn get_epoch_first_checkpoint_sequence_number(
        &mut self,
        epoch: u64,
    ) -> Result<u64, tonic::Status> {
        let request = crate::node::EpochRequest { epoch };
        let response = self
            .client
            .get_epoch_first_checkpoint_sequence_number(request)
            .await?;
        Ok(response.into_inner().sequence_number)
    }

    /// Deserialize checkpoint data based on the checkpoint type (full or summary).
    /// Returns either checkpoint data or summary depending on the checkpoint
    /// type.
    pub fn deserialize_checkpoint(
        checkpoint: &crate::node::Checkpoint,
    ) -> Result<CheckpointContent, Box<dyn std::error::Error + Send + Sync>> {
        let bcs_data = checkpoint
            .bcs_data
            .as_ref()
            .ok_or("Missing BCS data in checkpoint")?;

        if checkpoint.is_full {
            let checkpoint_data = bcs_data
                .deserialize_into::<CheckpointData>()?
                .into_v1()
                .ok_or("Unsupported checkpoint data version")?;
            Ok(CheckpointContent::Data(checkpoint_data))
        } else {
            let checkpoint_summary = bcs_data
                .deserialize_into::<CertifiedCheckpointSummary>()?
                .into_v1()
                .ok_or("Unsupported checkpoint summary version")?;
            Ok(CheckpointContent::Summary(checkpoint_summary))
        }
    }
}
