// Copyright (c) 2026 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

//! High-level API for object queries.

use iota_grpc_types::v0::{
    ledger_service::{GetObjectsRequest, ObjectRequest, ObjectRequests},
    types::ObjectReference,
};
use iota_sdk_types::{Object, ObjectId, Version};

use crate::{
    Client,
    api::{FieldMask, FieldMaskUtil, OBJECTS_READ_MASK, ProtoResult, Result, convert_object},
};

impl Client {
    /// Get objects by their IDs and optional versions.
    ///
    /// Each tuple contains (ObjectId, Option<Version>). If version is None,
    /// the latest version is returned.
    ///
    /// # Field Mask
    ///
    /// The optional `read_mask` parameter controls which fields the server
    /// returns. If `None`, uses [`OBJECTS_READ_MASK`] which includes all
    /// fields needed for `Object` deserialization.
    ///
    /// **Required fields** (must be included in custom masks):
    /// - `bcs` - Object BCS data
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use iota_grpc_client::Client;
    /// # use iota_sdk_types::ObjectId;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Client::connect("http://localhost:9000").await?;
    /// let object_id: ObjectId = "0x2".parse()?;
    ///
    /// // Default: get BCS data
    /// let objs = client.get_objects(&[(object_id, None)], None).await?;
    ///
    /// // With reference info (if you also want object metadata)
    /// let objs = client
    ///     .get_objects(&[(object_id, None)], Some("bcs,reference"))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_objects(
        &self,
        refs: &[(ObjectId, Option<Version>)],
        read_mask: Option<&str>,
    ) -> Result<Vec<Object>> {
        if refs.is_empty() {
            return Ok(vec![]);
        }

        let requests = ObjectRequests {
            requests: refs
                .iter()
                .map(|(id, version)| ObjectRequest {
                    object_ref: Some(ObjectReference {
                        object_id: Some(id.to_string()),
                        version: *version,
                        digest: None,
                    }),
                })
                .collect(),
        };

        let mask = read_mask.unwrap_or(OBJECTS_READ_MASK);
        let request = GetObjectsRequest {
            requests: Some(requests),
            read_mask: Some(FieldMask::from_str(mask)),
            max_message_size_bytes: None,
        };

        let mut client = self.ledger_service_client();

        let mut stream = client.get_objects(request).await?.into_inner();
        let mut results = Vec::with_capacity(refs.len());

        while let Some(response) = stream.message().await? {
            for result in response.objects {
                let proto_obj = result.into_result()?;
                let obj = convert_object(&proto_obj, "object.bcs")?;
                results.push(obj);
            }
        }

        Ok(results)
    }
}
