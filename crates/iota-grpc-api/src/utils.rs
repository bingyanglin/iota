// Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! Common utility functions for gRPC API services

use tonic::Status;

/// Helper function to serialize data to BCS and handle errors consistently
pub fn serialize_to_bcs<T: serde::Serialize>(
    data: &T,
    context: &str,
) -> Result<crate::common::BcsData, Status> {
    crate::common::BcsData::serialize_from(data)
        .map_err(|e| Status::internal(format!("{context} BCS serialization failed: {e}")))
}

/// Helper function to convert a collection to BCS serialized Vec
pub fn serialize_collection_to_bcs<T: serde::Serialize>(
    items: impl Iterator<Item = T>,
    context: &str,
) -> Result<Vec<crate::common::BcsData>, Status> {
    items
        .map(|item| serialize_to_bcs(&item, context))
        .collect::<Result<Vec<_>, _>>()
}

/// Helper function to deserialize bytes to any deserializable type
pub fn convert_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
    bcs::from_bytes(bytes)
        .map_err(|e| Status::invalid_argument(format!("Failed to deserialize: {e}")))
}
