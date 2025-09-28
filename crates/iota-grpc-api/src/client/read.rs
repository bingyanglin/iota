// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use tonic::transport::Channel;

use crate::read::{GetObjectRequest, GetObjectResponse, read_service_client::ReadServiceClient};

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
    /// * `request` - GetObject request containing object ID and display options
    ///
    /// # Returns
    /// GetObjectResponse containing object data or error information
    pub async fn get_object(
        &mut self,
        request: GetObjectRequest,
    ) -> Result<GetObjectResponse, tonic::Status> {
        let response = self.client.get_object(request).await?;
        Ok(response.into_inner())
    }
}
