// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use iota_json_rpc_types::{IotaObjectData, IotaObjectDataOptions, IotaObjectResponse};
use iota_types::{base_types::ObjectID, error::IotaObjectResponseError};
use tonic::{Status, transport::Channel};

use crate::{
    common::Address,
    read::{GetObjectRequest, ObjectDataOptions, read_service_client::ReadServiceClient},
};

/// Dedicated client for read-related gRPC operations.
///
/// This client handles all read service interactions including object retrieval
/// with various options for data display.
#[derive(Clone)]
pub struct ReadClient {
    client: ReadServiceClient<Channel>,
}

impl ReadClient {
    /// Create a new ReadClient from a shared gRPC channel.
    pub(super) fn new(channel: Channel) -> Self {
        Self {
            client: ReadServiceClient::new(channel),
        }
    }

    /// Get an object by ID with specified options.
    ///
    /// # Arguments
    /// * `object_id` - The object ID to retrieve
    /// * `options` - Options for what data to include in the response
    ///
    /// # Returns
    /// Result containing IotaObjectResponse (matches JSON-RPC API)
    pub async fn get_object(
        &mut self,
        object_id: ObjectID,
        options: Option<IotaObjectDataOptions>,
    ) -> Result<IotaObjectResponse, Status> {
        // Convert to gRPC request format
        let grpc_request = GetObjectRequest {
            object_id: Some(Address {
                address: object_id.into_bytes().to_vec(),
            }),
            options: options.map(|opts| ObjectDataOptions {
                show_type: opts.show_type,
                show_owner: opts.show_owner,
                show_previous_transaction: opts.show_previous_transaction,
                show_display: opts.show_display,
                show_content: opts.show_content,
                show_bcs: opts.show_bcs,
                show_storage_rebate: opts.show_storage_rebate,
            }),
        };

        // Make gRPC call
        let response = self.client.get_object(grpc_request).await?;

        let grpc_response = response.into_inner();

        // Deserialize JSON response
        Self::deserialize_response(&grpc_response)
    }

    /// Deserialize JSON response into IotaObjectResponse
    fn deserialize_response(
        response: &crate::read::GetObjectResponse,
    ) -> Result<IotaObjectResponse, Status> {
        // Check for success data first
        if let Some(data_wrapper) = &response.json_data {
            // Deserialize success data
            let object_data: IotaObjectData =
                serde_json::from_slice(&data_wrapper.data).map_err(|e| {
                    Status::internal(format!("Failed to deserialize object data from JSON: {e}"))
                })?;
            Ok(IotaObjectResponse::new_with_data(object_data))
        } else if let Some(error_wrapper) = &response.json_error {
            // Deserialize error
            let error: IotaObjectResponseError = serde_json::from_slice(&error_wrapper.data)
                .map_err(|e| {
                    Status::internal(format!("Failed to deserialize error from JSON: {e}"))
                })?;
            Ok(IotaObjectResponse::new_with_error(error))
        } else {
            Err(Status::internal("Response contains neither data nor error"))
        }
    }
}
